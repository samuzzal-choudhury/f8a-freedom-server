"""Definition of all REST API endpoints of the server module."""

import datetime
import functools
import uuid
import urllib

from io import StringIO

from requests_futures.sessions import FuturesSession
from flask import Blueprint, current_app, request, url_for, Response
from flask.json import jsonify
from flask_restful import Api, Resource, reqparse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from f8a_worker.models import (Ecosystem, StackAnalysisRequest)
from f8a_worker.manifests import get_manifest_descriptor_by_filename
from f8a_worker.utils import (MavenCoordinates, case_sensitivity_transform)

from . import rdb
from .dependency_finder import DependencyFinder
from .auth import auth_required, decode_token, get_access_token
from .exceptions import HTTPError
from .utils import (retrieve_worker_result, get_analyses_from_graph,
                    get_request_count, fetch_file_from_github_release,
                    get_item_from_list_by_key_value, RecommendationReason,
                    convert_resp_to_text, request_wants_json)
from .license_extractor import extract_licenses

import os
from .default_config import GEMINI_SERVER_URL


api_v1 = Blueprint('api_v1', __name__, url_prefix='/api/v1')
rest_api_v1 = Api(api_v1)

pagination_parser = reqparse.RequestParser()
pagination_parser.add_argument('page', type=int, default=0)
pagination_parser.add_argument('per_page', type=int, default=50)

ANALYSIS_ACCESS_COUNT_KEY = 'access_count'
TOTAL_COUNT_KEY = 'total_count'

original_handle_error = rest_api_v1.handle_error

ANALYTICS_API_VERSION = "v1.0"

worker_count = int(os.getenv('FUTURES_SESSION_WORKER_COUNT', '100'))
_session = FuturesSession(max_workers=worker_count)


# see <dir>.exceptions.HTTPError docstring
def handle_http_error(e):
    """Handle HTTPError exceptions."""
    if isinstance(e, HTTPError):
        res = jsonify({'error': e.error})
        res.status_code = e.status_code
        return res
    else:
        return original_handle_error(e)


@api_v1.route('/_error')
def error():
    """Implement the endpoint used by httpd, which redirects its errors to it."""
    try:
        status = int(request.environ['REDIRECT_STATUS'])
    except Exception:
        # if there's an exception, it means that a client accessed this directly;
        #  in this case, we want to make it look like the endpoint is not here
        return api_404_handler()
    msg = 'Unknown error'
    # for now, we just provide specific error for stuff that already happened;
    #  before adding more, I'd like to see them actually happening with reproducers
    if status == 401:
        msg = 'Authentication failed'
    elif status == 405:
        msg = 'Method not allowed for this endpoint'
    raise HTTPError(status, msg)


@api_v1.route('/readiness')
def readiness():
    """Handle the /readiness REST API call."""
    return jsonify({}), 200


@api_v1.route('/liveness')
def liveness():
    """Handle the /liveness REST API call."""
    # Check database connection
    current_app.logger.debug("Liveness probe - trying to connect to database "
                             "and execute a query")
    rdb.session.query(Ecosystem).count()
    return jsonify({}), 200


api_v1.coreapi_http_error_handler = handle_http_error
rest_api_v1.handle_error = handle_http_error


def get_item_skip(page, per_page):
    """Get the number of items to skip for the first page-1 pages."""
    return per_page * page


def get_item_relative_limit(page, per_page):
    """Get the maximum possible number of items on one page."""
    return per_page


def get_item_absolute_limit(page, per_page):
    """Get the total possible number of items."""
    return per_page * (page + 1)


def get_items_for_page(items, page, per_page):
    """Get all items for specified page and number of items to be used per page."""
    return items[get_item_skip(page, per_page):get_item_absolute_limit(page, per_page)]


def paginated(func):
    """Provide paginated output for longer responses."""
    @functools.wraps(func)
    def inner(*args, **kwargs):
        func_res = func(*args, **kwargs)
        res, code, headers = func_res, 200, {}
        if isinstance(res, tuple):
            if len(res) == 3:
                res, code, headers = func_res
            elif len(res) == 2:
                res, code = func_res
            else:
                raise HTTPError('Internal error', 500)

        args = pagination_parser.parse_args()
        page, per_page = args['page'], args['per_page']
        count = res[TOTAL_COUNT_KEY]

        previous_page = None if page == 0 else page - 1
        next_page = None if get_item_absolute_limit(page, per_page) >= count else page + 1

        view_args = request.view_args.copy()
        view_args['per_page'] = per_page

        view_args['page'] = previous_page
        paging = []
        if previous_page is not None:
            paging.append({'url': url_for(request.endpoint, **view_args), 'rel': 'prev'})
        view_args['page'] = next_page
        if next_page is not None:
            paging.append({'url': url_for(request.endpoint, **view_args), 'rel': 'next'})

        headers['Link'] = ', '.join(['<{url}>; rel="{rel}"'.format(**d) for d in paging])

        return res, code, headers

    return inner


# flask-restful doesn't actually store a list of endpoints, so we register them as they
#  pass through add_resource_no_matter_slashes
_resource_paths = []


def add_resource_no_matter_slashes(resource, route, endpoint=None, defaults=None):
    """Add a resource for both trailing slash and no trailing slash to prevent redirects."""
    slashless = route.rstrip('/')
    _resource_paths.append(api_v1.url_prefix + slashless)
    slashful = route + '/'
    endpoint = endpoint or resource.__name__.lower()
    defaults = defaults or {}

    rest_api_v1.add_resource(resource,
                             slashless,
                             endpoint=endpoint + '__slashless',
                             defaults=defaults)
    rest_api_v1.add_resource(resource,
                             slashful,
                             endpoint=endpoint + '__slashful',
                             defaults=defaults)


class ResourceWithSchema(Resource):
    """This class makes sure we can add schemas to any response returned by any API endpoint.

    If a subclass of ResourceWithSchema is supposed to add a schema, it has to:
    - either implement `add_schema` method (see its docstring for information on signature
      of this method)
    - or add a `schema_ref` (instance of `f8a_worker.schemas.SchemaRef`) class attribute.
      If this attribute is added, it only adds schema to response with `200` status code
      on `GET` request.
    Note that if both `schema_ref` and `add_schema` are defined, only the method will be used.
    """

    def add_schema(self, response, status_code, method):
        """Add schema to response.

        The schema must be dict containing 3 string values:
        name, version and url (representing name and version of the schema and its
        full url).

        :param response: dict, the actual response object returned by the view
        :param status_code: int, numeric representation of returned status code
        :param method: str, uppercase textual representation of used HTTP method
        :return: dict, modified response object that includes the schema
        """
        if hasattr(self, 'schema_ref') and status_code == 200 and method == 'GET':
            response['schema'] = {
                'name': self.schema_ref.name,
                'version': self.schema_ref.version,
                'url': PublishedSchemas.get_api_schema_url(name=self.schema_ref.name,
                                                           version=self.schema_ref.version)
            }
        return response

    def dispatch_request(self, *args, **kwargs):
        """Perform the request dispatching based on the standard Flask dispatcher."""
        response = super().dispatch_request(*args, **kwargs)

        method = request.method
        status_code = 200
        response_body = response
        headers = None

        if isinstance(response, tuple):
            response_body = response[0]
            if len(response) > 1:
                status_code = response[1]
            if len(response) > 2:
                headers = response[2]

        return self.add_schema(response_body, status_code, method), status_code, headers


@api_v1.route('/')
def base_url():
    return "Hello!!"


class ComponentAnalyses(ResourceWithSchema):
    """Implementation of all /component-analyses REST API calls."""

    method_decorators = [auth_required]

    @staticmethod
    def get(ecosystem, package, version):
        """Handle the GET REST API call."""
        decoded = decode_token()
        package = urllib.parse.unquote(package)
        if ecosystem == 'maven':
            package = MavenCoordinates.normalize_str(package)
        package = case_sensitivity_transform(ecosystem, package)
        result = get_analyses_from_graph(ecosystem, package, version)

        if result is not None:
            return result

        msg = "No data found for {ecosystem} package " \
              "{package}/{version}".format(ecosystem=ecosystem,
                                           package=package, version=version)
        raise HTTPError(404, msg)

    @staticmethod
    def post(ecosystem, package, version):
        """Handle the POST REST API call."""
        raise HTTPError(405, "Unsupported API endpoint.")


class StackAnalysesGET(ResourceWithSchema):
    """Implementation of the /stack-analyses GET REST API call method."""

    method_decorators = [auth_required]

    @staticmethod
    def get(external_request_id):
        """Handle the GET REST API call."""
        # TODO: reduce cyclomatic complexity
        if get_request_count(rdb, external_request_id) < 1:
            raise HTTPError(404, "Invalid request ID '{t}'.".format(t=external_request_id))

        graph_agg = retrieve_worker_result(rdb, external_request_id, "GraphAggregatorTask")
        if graph_agg is not None and 'task_result' in graph_agg:
            if graph_agg['task_result'] is None:
                raise HTTPError(500, 'Invalid manifest file(s) received. '
                                     'Please submit valid manifest files for stack analysis')

        stack_result = retrieve_worker_result(rdb, external_request_id, "stack_aggregator_v2")
        reco_result = retrieve_worker_result(rdb, external_request_id, "recommendation_v2")

        if stack_result is None and reco_result is None:
            raise HTTPError(202, "Analysis for request ID '{t}' is in progress".format(
                t=external_request_id))

        if stack_result == -1 and reco_result == -1:
            raise HTTPError(404, "Worker result for request ID '{t}' doesn't exist yet".format(
                t=external_request_id))

        started_at = None
        finished_at = None
        version = None
        release = None
        manifest_response = []
        stacks = []
        recommendations = []

        if stack_result is not None and 'task_result' in stack_result:
            started_at = stack_result.get("task_result", {}).get("_audit", {}).get("started_at",
                                                                                   started_at)
            finished_at = stack_result.get("task_result", {}).get("_audit", {}).get("ended_at",
                                                                                    finished_at)
            version = stack_result.get("task_result", {}).get("_audit", {}).get("version",
                                                                                version)
            release = stack_result.get("task_result", {}).get("_release", release)
            stacks = stack_result.get("task_result", {}).get("stack_data", stacks)

        if reco_result is not None and 'task_result' in reco_result:
            recommendations = reco_result.get("task_result", {}).get("recommendations",
                                                                     recommendations)

        if not stacks:
            return {
                "version": version,
                "release": release,
                "started_at": started_at,
                "finished_at": finished_at,
                "request_id": external_request_id,
                "result": manifest_response
            }
        for stack in stacks:
            user_stack_deps = stack.get('user_stack_info', {}).get('analyzed_dependencies', [])
            stack_recommendation = get_item_from_list_by_key_value(recommendations,
                                                                   "manifest_file_path",
                                                                   stack.get(
                                                                       "manifest_file_path"))
            for dep in user_stack_deps:
                # Adding topics from the recommendations
                if stack_recommendation is not None:
                    dep["topic_list"] = stack_recommendation.get("input_stack_topics",
                                                                 {}).get(dep.get('name'), [])
                else:
                    dep["topic_list"] = []

        for stack in stacks:
            stack["recommendation"] = get_item_from_list_by_key_value(
                recommendations,
                "manifest_file_path",
                stack.get("manifest_file_path"))
            manifest_response.append(stack)

        # Populate reason for alternate and companion recommendation
        manifest_response = RecommendationReason().add_reco_reason(manifest_response)

        resp = {
            "version": version,
            "release": release,
            "started_at": started_at,
            "finished_at": finished_at,
            "request_id": external_request_id,
            "result": manifest_response
        }

        if request_wants_json(request):
            # client requests with Accept header application/json
            return resp

        return convert_resp_to_text(resp)

    @staticmethod
    def post(ecosystem, package, version):
        """Handle the POST REST API call."""
        raise HTTPError(405, "Unsupported API endpoint.")


class StackAnalyses(ResourceWithSchema):
    """Implementation of all /stack-analyses REST API calls."""

    method_decorators = [auth_required]

    @staticmethod
    def post():
        """Handle the POST REST API call."""
        # TODO: reduce cyclomatic complexity
        decoded = decode_token()
        github_token = get_access_token('github')
        sid = request.args.get('sid')
        license_files = list()
        check_license = request.args.get('check_license', 'false') == 'true'
        github_url = request.form.get("github_url")
        ref = request.form.get('github_ref')
        user_email = request.headers.get('UserEmail')
        scan_repo_url = request.headers.get('ScanRepoUrl')

        headers = 'HEADER INFO: %r' % request.headers

        if not user_email:
            user_email = decoded.get('email', 'bayesian@redhat.com')

        if scan_repo_url:
            try:
                api_url = GEMINI_SERVER_URL
                dependency_files = request.files.getlist('dependencyFile[]')
                current_app.logger.info('%r' % dependency_files)
                data = {'git-url': scan_repo_url,
                        'email-ids': [user_email]}
                if dependency_files:
                    files = list()
                    for dependency_file in dependency_files:

                        # http://docs.python-requests.org/en/master/user/advanced/#post-multiple-multipart-encoded-files
                        files.append((
                                dependency_file.name, (
                                    dependency_file.filename,
                                    dependency_file.read(),
                                    'text/plain'
                                )
                        ))

                    _session.headers['Authorization'] = request.headers.get('Authorization')
                    _session.post('{}/api/v1/user-repo/scan/experimental'.format(api_url),
                                  data=data, files=files)
                else:
                    _session.headers['Authorization'] = request.headers.get('Authorization')
                    _session.post('{}/api/v1/user-repo/scan'.format(api_url), json=data)
            except Exception as exc:
                raise HTTPError(500, "Could not process the scan endpoint call") \
                    from exc

        source = request.form.get('source')
        if github_url is not None:
            files = fetch_file_from_github_release(url=github_url,
                                                   filename='pom.xml',
                                                   token=github_token.get('access_token'),
                                                   ref=ref)
        else:
            files = request.files.getlist('manifest[]')
            filepaths = request.values.getlist('filePath[]')
            license_files = request.files.getlist('license[]')

            current_app.logger.info('%r' % files)
            current_app.logger.info('%r' % filepaths)

            # At least one manifest file path should be present to analyse a stack
            if not filepaths:
                raise HTTPError(400, error="Error processing request. "
                                           "Please send a valid manifest file path.")
            if len(files) != len(filepaths):
                raise HTTPError(400, error="Error processing request. "
                                           "Number of manifests and filePaths must be the same.")

        # At least one manifest file should be present to analyse a stack
        if not files:
            raise HTTPError(400, error="Error processing request. "
                                       "Please upload a valid manifest files.")
        dt = datetime.datetime.now()
        if sid:
            request_id = sid
            is_modified_flag = {'is_modified': True}
        else:
            request_id = uuid.uuid4().hex
            is_modified_flag = {'is_modified': False}

        iso = datetime.datetime.utcnow().isoformat()

        manifests = []
        ecosystem = None
        for index, manifest_file_raw in enumerate(files):
            if github_url is not None:
                filename = manifest_file_raw.get('filename', None)
                filepath = manifest_file_raw.get('filepath', None)
                content = manifest_file_raw.get('content')
            else:
                filename = manifest_file_raw.filename
                filepath = filepaths[index]
                content = manifest_file_raw.read().decode('utf-8')

            # check if manifest files with given name are supported
            manifest_descriptor = get_manifest_descriptor_by_filename(filename)
            if manifest_descriptor is None:
                raise HTTPError(400, error="Manifest file '{filename}' is not supported".format(
                    filename=filename))

            # In memory file to be passed as an API parameter to /appstack
            manifest_file = StringIO(content)

            # Check if the manifest is valid
            if not manifest_descriptor.validate(content):
                raise HTTPError(400, error="Error processing request. Please upload a valid "
                                           "manifest file '{filename}'".format(filename=filename))

            # Record the response details for this manifest file
            manifest = {'filename': filename,
                        'content': content,
                        'ecosystem': manifest_descriptor.ecosystem,
                        'filepath': filepath}

            manifests.append(manifest)

        data = {'api_name': 'stack_analyses',
                'user_email': user_email,
                'user_profile': decoded}
        args = {'external_request_id': request_id,
                'ecosystem': ecosystem, 'data': data}

        try:
            api_url = current_app.config['F8_API_BACKBONE_HOST']

            d = DependencyFinder()
            deps = d.execute(args, rdb.session, manifests, source)
            deps['external_request_id'] = request_id
            deps['current_stack_license'] = extract_licenses(license_files)
            deps.update(is_modified_flag)

            _session.post(
                '{}/api/v1/stack_aggregator'.format(api_url), json=deps,
                params={'check_license': str(check_license).lower()})
            _session.post('{}/api/v1/recommender'.format(api_url), json=deps,
                          params={'check_license': str(check_license).lower()})

        except Exception as exc:
            raise HTTPError(500, ("Could not process {t}."
                                  .format(t=request_id))) from exc
        try:
            insert_stmt = insert(StackAnalysisRequest).values(
                id=request_id,
                submitTime=str(dt),
                requestJson={'manifest': manifests, 'header_info': headers},
                dep_snapshot=deps
            )
            do_update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['id'],
                set_=dict(dep_snapshot=deps)
            )
            rdb.session.execute(do_update_stmt)
            rdb.session.commit()
            return {"status": "success", "submitted_at": str(dt), "id": str(request_id)}
        except SQLAlchemyError as e:
            raise HTTPError(500, "Error updating log for request {t}".format(t=sid)) from e

    @staticmethod
    def get():
        """Handle the GET REST API call."""
        raise HTTPError(405, "Unsupported API endpoint")


add_resource_no_matter_slashes(ComponentAnalyses, '/component-analyses')
add_resource_no_matter_slashes(StackAnalyses, '/stack-analyses')
add_resource_no_matter_slashes(StackAnalysesGET, '/stack-analyses/<external_request_id>')


# workaround https://github.com/mitsuhiko/flask/issues/1498
# NOTE: this *must* come in the end, unless it'll overwrite rules defined
# after this


@api_v1.route('/<path:invalid_path>')
def api_404_handler(*args, **kwargs):
    """Handle all other routes not defined above."""
    return jsonify(error='Cannot match given query to any API v1 endpoint'), 404

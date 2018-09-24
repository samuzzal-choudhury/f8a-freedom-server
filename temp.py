qstring = ("g.V().has('pecosystem','" + "maven" + "').has('pname','" + "io.vertx:vertx-core" + "').has('version','" + "3.4.1" + "').")
qstring += ("as('version').in('has_version').as('package')." + "select('version','package').by(valueMap());")
payload = {'gremlin': qstring}

print (payload)

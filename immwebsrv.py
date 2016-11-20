import logging, cherrypy, os
from pathlib import Path
from datetime import timedelta
from pytz import timezone
from aclib.ops4app import Ops4app
from aclib.func4strings import Func4strings as f4s
import rethinkdb as r
import hashlib

class ServImm(object):
    def __init__(self, theops):
        self.myops = theops

    # ------- JSON : Recup des stats initiales et du user qui est authent
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def get_init(self, _="", nb_days="7"):
        retourObj = dict()
        retourObj['User'] = cherrypy.session.get('usrun') or 'Nobody'
        retourObj['DateMin'] = retourObj['DateInterval'] = retourObj['DateMax'] = 'yy-mm-dd hh:mm'
        retourObj['CountTotal'] = retourObj['CountSelected'] = "0"
        try :
            nb_days2 = int(nb_days)
        except :
            nb_days2 = 7
        if retourObj['User'] != 'Nobody' :
            if self.myops.rdb_get_lock() is not None :
                DateMin  = r.table('immoanno')['ts_updated'].min().run(self.myops.rdb)
                retourObj['DateMin'] = DateMin.astimezone(tz=timezone('Europe/Paris')).strftime('%y-%m-%d %H:%M')

                DateMax  = r.table('immoanno')['ts_updated'].max().run(self.myops.rdb)
                retourObj['DateMax'] = DateMax.astimezone(tz=timezone('Europe/Paris')).strftime('%y-%m-%d %H:%M')

                retourObj['CountTotal'] = int(r.table('immoanno').count().run(self.myops.rdb))
                DateLimite = DateMax - timedelta(days=int(nb_days2))
                retourObj['CountSelected'] = int(r.table('immoanno').filter(lambda row : row["ts_updated"].ge(DateLimite)).count().run(self.myops.rdb))

                DateMinSelect  = r.table('immoanno').filter(lambda row : row["ts_updated"].ge(DateLimite))['ts_updated'].min().run(self.myops.rdb)
                retourObj['DateInterval'] = DateMinSelect.astimezone(tz=timezone('Europe/Paris')).strftime('%y-%m-%d %H:%M')

                self.myops.rdb_release()
        return retourObj

    # ------- JSON : Recup des stats = DateMin DateMax CountAll CountSelected
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def get_usrtags(self, _=""):
        retourObj = dict()
        retourObj['alltags'] = list()
        retourObj['showtags'] = list()
        retourObj['hidetags'] = list()
        theuser = cherrypy.session.get('usrun') or 'Nobody'
        if theuser != 'Nobody' :
            if self.myops.rdb_get_lock() is not None :
                if r.table('immoanno_users')['tags_usr'][theuser].count().run(self.myops.rdb) > 0 :
                    retourObj['alltags'] = r.table('immoanno_users')['tags_usr'][theuser].distinct().reduce(lambda left,right : left+right).distinct().run(self.myops.rdb)
                self.myops.rdb_release()
                if "---" not in retourObj['alltags'] :
                    retourObj['alltags'].append("---")
                if "---" not in retourObj['hidetags'] :
                    retourObj['hidetags'].append("---")

            cherrypy.session['alltags'] = retourObj['alltags']

        return retourObj

    # ------- Update les tags sur un objet pour un user specifique
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def upd_obj_tags(self, _="", object_id="", str_tags_comma=""):
        theuser = cherrypy.session.get('usrun') or 'Nobody'
        if theuser != "Nobody" and object_id != "" :
            tags_obj = str_tags_comma.split(',')
            if tags_obj[0] == "" :
                tags_obj2 = []
            else :
                tags_obj2 = [f4s.cleanOnlyLetterDigit(x).lower() for x in tags_obj]
                # Au besoin on ajoute un nouveau tag a la session pour refresh de la liste
                tags_in_session = list(set().union((cherrypy.session.get('alltags') or ["---"]), tags_obj2))
                cherrypy.session['alltags'] = tags_in_session

            if self.myops.rdb_get_lock() is not None :
                curseur = r.table('immoanno_users').insert({"id": object_id, "tags_usr": {theuser: tags_obj2}}, conflict='update').run(self.myops.rdb)
                logging.debug('Update tags : %s' % str(curseur))
                self.myops.rdb_release()
        return { 'answer' : "ok"}

    # ------- Recup de la liste globale
    @cherrypy.expose()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def get_liste(self, _="", nb_days="7"):
        retourObj = dict()
        retourObj['User'] = cherrypy.session.get('usrun') or 'Nobody'
        retourObj['DateMin'] = retourObj['DateInterval'] = retourObj['DateMax'] = 'yy-mm-dd hh:mm'
        retourObj['CountTotal'] = retourObj['CountSelected'] = "0"
        try :
            nb_days2 = int(nb_days)
        except :
            nb_days2 = 7
        theuser = cherrypy.session.get('usrun') or 'Nobody'
        if retourObj['User'] != 'Nobody' :
            if self.myops.rdb_get_lock() is not None :
                # --- Requete avec JOINTURE sur les user tags
                DateMax  = r.table('immoanno')['ts_updated'].max().run(self.myops.rdb)
                DateLimite = DateMax - timedelta(days=int(nb_days2))
                curseur = r.table('immoanno').filter(lambda row : row["ts_updated"].ge(DateLimite))
                curseur = curseur.outer_join(r.table('immoanno_users').filter(lambda row : row["tags_usr"].keys().contains(theuser)), lambda rowA,rowB: rowB['id'].eq(rowA['id']).and_(rowB.has_fields('tags_usr'))).zip()
                # Choix des champs a garder
                curseur = curseur.order_by(r.desc('ts_updated'), 'codepostal')
                curseur = curseur.run(self.myops.rdb)

                #--- fabrication de la liste des tags existant pour tous les selectize (chqaue ligne)
                liste_tags_connus = (cherrypy.session.get('alltags') or ['---'])
                liste_tags_options_str = "["
                tagcount=0
                for tag in liste_tags_connus :
                    if tagcount > 0 :
                        liste_tags_options_str += ','
                    tagcount += 1
                    liste_tags_options_str += "{value:'%s', text:'%s'}" % (tag, tag)
                liste_tags_options_str += ']'

                # --- boucle sur les lignes de la datatable
                laliste = list()
                for doc in curseur :
                    objj = dict()
                    docidh = doc.get('id_hash') or hashlib.sha1(str(doc['id']).encode('utf-8')).hexdigest()

                    # -------- ts_updated
                    objj['ts_updated'] =  doc['ts_updated'].strftime('%y-%m-%d<br>%H:%M')

                    # -------- localite
                    objj['localite'] = '<p align="center"><a href="https://www.google.fr/maps/place/%s" target="_blank">%s</a><br><b>%s</b></p>' % ((doc.get('codepostal') or '00000'), (doc.get('codepostal') or '00000'), (doc.get('localite_stz') or 'Ville inconnue'))

                    # -------- tags_usr : seront inclus plus loin
                    liste_tags_selected = "["
                    if (doc.get('tags_usr') or None) is not None :
                        if theuser in doc.get('tags_usr') :
                            tagcount=0
                            for tag in doc['tags_usr'][theuser] :
                                if tagcount > 0 :
                                    liste_tags_selected += ','
                                tagcount += 1
                                liste_tags_selected += "'%s'" % tag
                    liste_tags_selected += ']'

                    # creation du code html avec SELECTIZE
                    id_of_input = "input_str" + docidh # "input_usr_" + str(doc['id'])
                    code_htmljs = '<div><input type="text" id="%s">' % id_of_input  # style="max-width:250px ;"
                    code_htmljs += '<script type="text/javascript">'
                    code_htmljs += "$('#%s').selectize({" % id_of_input
                    code_htmljs += "plugins: ['restore_on_backspace','remove_button', 'drag_drop'], "
                    code_htmljs += "delimiter: ',', "
                    code_htmljs += "options : %s, " % liste_tags_options_str
                    code_htmljs += "items : %s, " % liste_tags_selected
                    code_htmljs += "persist: true, "
                    code_htmljs += "create: function(input) { return { value: input, text: input } },"
                    code_htmljs += 'onChange: function(value) {tags_modif("'+str(doc['id'])+'", "'+id_of_input+'", value);}'
                    code_htmljs += "});</script></div>"

                    # -------- Commandes
                    code_htmljs2 = '<div style="max-width:20 ; text-align: center ;">'
                    code_htmljs2 += '<button type="button" class="btn btn-danger btn-sm" style="margin-top:4px;" onclick="tags_add_del(\'%s\', \'%s\');">Del</button>' % (str(doc['id']), id_of_input)

                    # effacer 1 ligne dans datatable : https://datatables.net/reference/api/row().remove()
                    code_htmljs2 += '</div>'
                    objj['commandes'] = code_htmljs2

                    # -------- title
                    tmpURLinterne  = "./dump_obj?pIDH=%s" % (doc.get('id_hash') or 'None')
                    objj['title']  =  '<p><b><a href="%s" title="" target="_blank">%s</a></b>&nbsp;' % (tmpURLinterne, (doc.get('title') or 'Titre non specifie'))
                    objj['title'] += '<br><i>par %s&nbsp;&nbsp;|&nbsp;&nbsp;%s&nbsp;&nbsp;|&nbsp;&nbsp;<a href="%s" target="_blank">lien</a></i></p>' % ((doc.get('uploadby') or ''), str(" ".join(sorted(doc.get('sources') or []))), (doc.get('url_annonce') or 'Url non trouvee'))
                    objj['title'] += code_htmljs

                    # -------- price
                    objj['price'] = '<p align="center">{:,d} k</p>'.format(int(round((doc.get('price') or 0)/1000,0)))

                    # -------- surface
                    objj['surface'] = '<p align="center">%d</p>' % (doc.get('surface') or 0)

                    # -------- Champ Description avec Images & Historique
                    retour="<p>"
                    tmpNbImg=0
                    for img_id in (doc.get('images_ids') or []) :
                        lien = "./dump_img?pID=%s" % img_id
                        tmpNbImg += 1
                        if tmpNbImg > 3 :
                            retour += '..'
                            break
                        else :
                            retour += '<a href="%s" target="_blank"><img src="%s" alt="" height="80" width="80"></a> ' % (lien, lien)

                    # Insertion des commentaires sous les images
                    retour += '\n<script type="text/javascript">function ShowHide(id) { var obj = document.getElementById(id); if(obj.className == "showobject") { obj.className = "hideobject"; } else { obj.className = "showobject"; } }</script>\n'
                    retour += '<style type="text/css"> .hideobject{ display: none; } .showobject{ display: block; } </style>\n'

                    # Description
                    tmpId1 = doc['ts_updated'].astimezone(tz=timezone('Europe/Paris')).strftime('%a%d%b%H%M') + doc['title_stz'] + "description"
                    retour += """<div onclick="ShowHide('""" + tmpId1 + """')"><b>Description</b></div>"""
                    retour += '<div id="%s" class="hideobject">%s</div>\n' % (tmpId1, doc['description'])

                    # Historique
                    tmpHist = ''
                    for histoKey in sorted(doc.get('history', {}).keys(),reverse=True) :
                        tmpHist += '<p><b>%s</b><br>%s</p>' % (histoKey, doc['history'][histoKey])
                    if tmpHist != '' :
                        tmpId2 = doc['ts_updated'].astimezone(tz=timezone('Europe/Paris')).strftime('%a%d%b%H%M') + doc['title_stz'] + "history"
                        retour += """<div onclick="ShowHide('""" + tmpId2 + """')"><b>History</b></div>"""
                        retour += '<div id="%s" class="hideobject">%s</div>\n' % (tmpId2, tmpHist)
                    retour+="</p>"
                    objj['description'] = str(retour)

                    laliste.append(objj)
                retourObj["data"] = laliste
                self.myops.rdb_release()
        return retourObj

    # ------- dump html
    @cherrypy.expose()
    def dump_obj(self, pIDH=''):  # , pParam=''):
        retourObj = '<html lang="en"><head><meta charset="UTF-8"><title>Annonce</title></head><body>'
        retourObj += '<table>'
        tmpImages = ""
        tmpFin = ""

        if pIDH != '' :
            dbconn = self.myops.rdb_get_lock()
            if dbconn is not None :
                curseur = r.table('immoanno')  # .max('ts_collected').to_json()
                curseur = curseur.filter(r.row["id_hash"].eq(pIDH))
                curseur = curseur.run(dbconn)
                tags0 = curseur.next()

                if tags0 :
                    curseur.close()

                    # -- Le html pour les champs importants
                    retourObj += '<tr><td><b>Titre</b></td><td><b>%s</b></td></tr>\n' % tags0.get('title', 'Pas de titre...')
                    retourObj += '<tr><td><b>Localite</b></td><td>%s - %s</td></tr>\n' % (tags0.get('localite_stz', 'Inconnu'), tags0.get('codepostal', '00000'))
                    retourObj += '<tr><td><b>Prix</b></td><td>%s</td></tr>\n' % tags0.get('price', '0')
                    retourObj += '<tr><td><b>Auteur</b></td><td>%s</td></tr>\n' % tags0.get('uploadby', 'Inconnu')
                    retourObj += '<tr><td><b>Description</b></td><td>%s</td></tr>\n' % tags0.get('description', 'Vide')

                    # -- Le html pour les images
                    for cle in sorted(tags0) :
                        if tags0[cle] != '' and tags0[cle] != '0' :
                            if 'images_ids' in cle :
                                for img_id in tags0[cle] :
                                    lien = "./dump_img?pID=%s" % img_id
                                    tmpImages += '<img src="%s" alt="">&nbsp;' % lien
                            elif cle not in ['codepostal', 'description', 'title', 'localite_stz', 'price', 'uploadby']:
                                tmpFin += '<tr><td>'
                                tmpFin += cle + "</td><td>" + str(tags0[cle]) + "</td></tr>"

                    if len(tmpImages) > 3 :
                        retourObj += '</table><br>' + tmpImages + '<table>'

                    # -- Le html pour les autres champs
                    if len(tmpFin) > 3 :
                        retourObj += tmpFin

            self.myops.rdb_release()
        retourObj += '</table>\n'
        retourObj += "</body></html>"
        return retourObj

    # ------- dump binaire
    @cherrypy.expose()
    def dump_img(self, pID=''):
        retourObj = ''
        if pID != '' :
            dbconn = self.myops.rdb_get_lock()
            if dbconn is not None :
                curseur = r.table('immoimg')
                curseur = curseur.get(pID)
                curseur = curseur.run(dbconn)

                cherrypy.response.headers['Content-Type'] = "image/" + curseur['type']
                retourObj = curseur['content']
            self.myops.rdb_release()

        return retourObj

if __name__ == '__main__':
    # --- Logs Definition  logging.Logger.manager.loggerDict.keys()
    Level_of_logs = level =logging.INFO
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=Level_of_logs, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(name)s|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.info("Starting from %s" % str(os.getcwd()))

    ops = Ops4app.get_instance(appli_name="immoweb", config_default_file_path='./immtrk.config.defaults.toml')
    if not ops :
        logging.critical('Problem with connexion to DB, exiting...')
    else :
        theusers = ops.cfg.get('websrv').get('users')
        def validate_password(realm='localhost', username='', password=''):
            if 1 < len(username) < 20 and 1 < len(password) < 20 :
                userh = hashlib.sha256(str(username).encode('utf-8')).hexdigest()
                passh = hashlib.sha256(str(password).encode('utf-8')).hexdigest()
                if userh in theusers and theusers[userh] == passh:
                    cherrypy.session['usrun'] = username
                    return True
            return False

        # http://docs.cherrypy.org/en/latest/pkg/cherrypy.html?highlight=ssl#cherrypy._cpserver.Server
        server_config={
            'server.socket_host'            : '0.0.0.0',
            'server.socket_port'            : int(ops.cfg.get('websrv').get('network.port')),
            'server.socket_queue_size'      : 5, # The ‘backlog’ argument to socket.listen(); specifies the maximum number of queued connections (default 5).
            'server.socket_timeout'         : 10, # The timeout in seconds for accepted connections (default 10).
            'server.accepted_queue_size'    : 50, # The maximum number of requests which will be queued up before the server refuses to accept it (default -1, meaning no limit).
            'server.thread_pool'            : 10, # The number of worker threads to start up in the pool.
            'server.thread_pool_max'        : 40, # he maximum size of the worker-thread pool. Use -1 to indicate no limit.

            'server.ssl_module'             : 'builtin', # ''pyopenssl' PAS COMPATIBLE PYHON 3 nov-2016, #'builtin', # The name of a registered SSL adaptation module to use with the builtin WSGI server. Builtin options are ‘builtin’ (to use the SSL library built into recent versions of Python) and ‘pyopenssl’ (to use the PyOpenSSL project, which you must install separately). You may also register your own classes in the wsgiserver.ssl_adapters dict.
            'server.ssl_private_key'        : '../immtrk-ress/immtrk2_cp.pem', # The filename of the private key to use with SSL.
            'server.ssl_certificate'        : '../immtrk-ress/immtrk2_cert.pem', # The filename of the SSL certificate to use.
            'server.ssl_certificate_chain'  : None, # When using PyOpenSSL, the certificate chain to pass to Context.load_verify_locations.
            'server.ssl_context'            : None, # When using PyOpenSSL, an instance of SSL.Context.

            'log.screen' : False, 'log.access_file': '' , 'log.error_file': '',
            'engine.autoreload.on' : False,  # Sinon le server se relance des qu'un fichier py est modifie...
        }
        cherrypy.config.update(server_config)

        app_conf = { '/': { 'tools.staticdir.on'  : True, 'tools.staticdir.index'  : "index.html", 'tools.staticdir.dir'            : Path().cwd().joinpath("websrv").as_posix(),
                            'tools.auth_basic.on' : True, 'tools.auth_basic.realm' : 'localhost',  'tools.auth_basic.checkpassword' : validate_password,
                            'tools.sessions.on'   : True } }
        cherrypy.tree.mount(ServImm(ops), "/", app_conf)

        # --- Loglevel pour CherryPy : a faire une fois les serveurs mounted et avant le start
        for log_mgt in logging.Logger.manager.loggerDict.keys() :
            if "cherrypy.access" in log_mgt :
                logging.getLogger(log_mgt).setLevel(logging.WARNING)

        # -------- Lancement --------
        cherrypy.engine.start()
        cherrypy.engine.block()

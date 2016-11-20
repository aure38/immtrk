# coding: utf8
from datetime import datetime
from pytz import timezone
from bs4 import BeautifulSoup
import rethinkdb as r, logging, time, os, re, random, fuzzywuzzy, schedule, difflib, hashlib, string
from fuzzywuzzy import process
import requests,threading, urllib.request  # urllib.request est utilise pour faire des manip sur les url, utiliser requests pour les appels
from aclib.ops4app import Ops4app
from aclib.func4strings import Func4strings as f4s

class ImmoFetch(threading.Thread):
    our_ville2codep = dict()    # str(nom_ville) -> str(codepostal)
    our_villenoms   = list()    # liste des noms de villes pour eviter de faire un .keys() a chaque fois
    our_codep2ville = dict()    # str(codepostal) -> str(nom_ville)
    our_nb_threads_running = 0
    our_ops = None              # Instance ops4app communes a tous les threads

    # -- Specify ops instance for all threads and other processings
    @staticmethod
    def init_static(ops_pointeur):
        # -- Assignation du ops
        if ImmoFetch.our_ops is None :
            ImmoFetch.our_ops = ops_pointeur
        # -- construction du dico commun a toutes les instances
        if len(ImmoFetch.our_ville2codep) == 0 :
            liste_obj = list(r.table('villesfr').filter(lambda row : r.expr(['01','07','38','73','74']).contains(row['departement'])).pluck('codepostal', 'nom_stz').run(ImmoFetch.our_ops.rdb))
            for obj in liste_obj:
                ImmoFetch.our_ville2codep[f4s.cleanMax(obj['nom_stz'])] = obj['codepostal']
                ImmoFetch.our_codep2ville[obj['codepostal']] = f4s.cleanMax(obj['nom_stz'])
            ImmoFetch.our_villenoms = sorted(ImmoFetch.our_ville2codep.keys())
    # -- Count threads & Log in KPI the start of the 1st thread
    @staticmethod
    def log_start_of_first_thread():
        ImmoFetch.our_nb_threads_running += 1
        if ImmoFetch.our_nb_threads_running == 1 :
            ImmoFetch.our_ops.insertKPI(measurement='state', value=1.0, tags={'component' : 'python'})
    # -- Count threads & Log in KPI the end of the last thread
    @staticmethod
    def log_end_of_last_thread():
        ImmoFetch.our_nb_threads_running -= 1
        if ImmoFetch.our_nb_threads_running == 0 :
            ImmoFetch.our_ops.insertKPI(measurement='state', value=0.0, tags={'component' : 'python'})
    # -- Creation d'un item avec champs par defaut
    @staticmethod
    def get_empty_obj() :
        tmpNow = datetime.now()
        localtz = timezone('Europe/Paris')
        defautdateiso = r.iso8601(localtz.localize(tmpNow).isoformat())
        return dict({'classeenergie': '',
              'codepostal': 'inconnu',
              'description': '',
              'description_stz': '',
              'ges': '',
              'history': {},
              'id': '',
              'id_hash': '',
              'images_ids': [],
              'localite_stz': 'inconnu',
              'nbpieces': 0,
              'price': 0,
              'sources': [],
              'surface': 0,
              'surface_terrain' : 0,
              'title': 'inconnu',
              'title_stz': 'inconnu',
              'ts_collected': defautdateiso,
              'ts_lastfetched': defautdateiso,
              'ts_published': defautdateiso,
              'ts_updated': defautdateiso,
              'type2bien': 'inconnu',
              'uploadby': '',
              'url_annonce': '',
              'url_images': []
              # 'user_tags': []
              })

    def __init__(self, nom4urls):
        super(ImmoFetch, self).__init__(group=None, target=None, daemon=False)  # name=None
        self.my_name = nom4urls

    # -- Insert ou update les img dans rdb et renvoie la liste des IDs inseres (avec ceux deja dans la db)
    @staticmethod
    def push_images_to_rdb(obj):
        liste_img_ids = list(obj['images_ids'])  # on recup les ids des imgs deja dans la base
        logging.debug('Downloading+insert de %d images' % len(obj['url_images']))
        for url_image in obj['url_images'] :
            obj_image = dict()
            obj_image['ts_created'] = obj['ts_collected']
            obj_image['annonce_id'] = obj['id']
            obj_image['url'] = url_image
            # creation de l'id a partir de l'url de l'image et de l'id de l'annonce (1 meme URL pour plusieurs annonce pourrait arriver ?)
            obj_image['id'] = hashlib.sha1(str(obj['id']+url_image).encode('utf-8')).hexdigest()
            obj_image['type'] = 'jpg'
            if   '.jpg' in obj_image['url'] or '.jpeg' in obj_image['url'] :        obj_image['type'] = 'jpg'
            elif '.png' in obj_image['url'] :                                       obj_image['type'] = 'png'
            elif '.gif' in obj_image['url'] :                                       obj_image['type'] = 'gif'
            else : logging.warning("Type d'image non reconnu, fixe a jpg pour : %s" % obj_image['url'])
            # - Download
            try :
                reqi = requests.get(url_image, timeout=20)
                obj_image['content'] = r.binary(reqi.content)
            except Exception as e :
                logging.error("Erreur durant le download de l'image : %s | %s" % (url_image, str(e)))
            else :
                # - Insert/Update in RDB
                reponse = r.table('immoimg').insert(obj_image, conflict="replace").run(ImmoFetch.our_ops.rdb_get_lock()) # conflict="update" ou replace ou error
                ImmoFetch.our_ops.rdb_release()
                if reponse['inserted'] == 1 or reponse['replaced'] == 1 or reponse['unchanged'] == 1:
                    # logging.debug("Insertion in DB Image : URL=%s" % obj_image['url'])
                    # try :
                    #     obj_images_key = reponse['generated_keys'][0]
                    #     if obj_images_key != '' :
                    #         obj['images_ids'].append(obj_images_key)
                    # except:
                    #     logging.error("Images inserees mais pas de key recuperee, Id = %s, Reponse = %s" % (obj['id'],str(reponse)))
                    liste_img_ids.append(obj_image['id'])
                else:
                    logging.error("Erreur sur l'insertion d'une image, Id = %s, Reponse = %s" % (obj['id'],str(reponse)))
                time.sleep(random.randint(1,3))    # Attente avant image suivante

        liste_img_ids = list(set(liste_img_ids))
        return liste_img_ids
    # -- Insert img dans la db
    @staticmethod
    def push_item_to_rdb(anno):
        retour = 'untouched' # or 'updated' or 'inserted'
        obj = dict(anno)
        # ---- Fabrication de l'ID L'id dans rethinkDB est limite a 127 caracteres
        obj['id_hash'] = hashlib.sha1(str(obj['url_annonce']).encode('utf-8')).hexdigest()
        obj['id'] = obj['id_hash']

        # On recupere l'objet s'il est deja dans la db (c'est la majorite des cas si update regulier)
        tmpAnnonceInDB = r.table('immoanno').get(obj['id']).run(ImmoFetch.our_ops.rdb_get_lock())
        ImmoFetch.our_ops.rdb_release()

        # ---- Nouvelle annonce
        if tmpAnnonceInDB is None :
            # ----- Insertion des images ---
            obj['images_ids'] = ImmoFetch.push_images_to_rdb(obj)

            #--- Insertion de l'annonce
            reponse = r.table('immoanno').insert(obj, conflict="error").run(ImmoFetch.our_ops.rdb_get_lock()) # conflict="update" ou replace
            ImmoFetch.our_ops.rdb_release()
            if reponse['inserted'] == 1 :
                retour = 'inserted'
            else:
                logging.error("Impossible d'inserer une annonce dans la db, Id = %s" % obj['id'])
        # ---- Deja dans la DB, Update necessaire ?
        else:
            #--- S'il y a deja un id dans la base : on merge certains champs comme le last time seen
            # on mergera toujours le last_seen, mais ce n'est pas un update en tant que tel
            tmpAnnonceInDB['ts_lastfetched'] = obj['ts_lastfetched']
            # Nouveau prix ?
            if tmpAnnonceInDB['price'] != obj['price'] :
                tmpHistoKey   = obj['ts_lastfetched'].run(ImmoFetch.our_ops.rdb_get_lock()).strftime('%y%m%d-%H%M%S') + " - Price"
                ImmoFetch.our_ops.rdb_release()
                tmpHistoValue = "%d" % tmpAnnonceInDB['price']
                tmpAnnonceInDB['price'] = obj['price']
                tmpAnnonceInDB['history'][tmpHistoKey] = tmpHistoValue
                tmpAnnonceInDB['ts_updated']   = obj['ts_lastfetched']
                retour = 'updated'

            # Nouveau titre : on calcule un taux de difference pour ne pas genere des updates sur un detail.
            if difflib.SequenceMatcher(a=tmpAnnonceInDB['title_stz'].lower(), b=obj['title_stz'].lower()).ratio() < 0.85 :
                tmpHistoKey   = obj['ts_lastfetched'].run(ImmoFetch.our_ops.rdb_get_lock()).strftime('%y%m%d-%H%M%S') + " - Titre"
                ImmoFetch.our_ops.rdb_release()
                tmpHistoValue = "%s" % tmpAnnonceInDB['title']
                tmpAnnonceInDB['title']     = obj['title']
                tmpAnnonceInDB['title_stz'] = obj['title_stz']
                tmpAnnonceInDB['history'][tmpHistoKey] = tmpHistoValue
                tmpAnnonceInDB['ts_updated']   = obj['ts_lastfetched']
                retour = 'updated'

            # Nouvelle description ?
            if difflib.SequenceMatcher(a=tmpAnnonceInDB['description_stz'].lower(), b=obj['description_stz'].lower()).ratio() < 0.8 :
                tmpHistoKey                             = obj['ts_lastfetched'].run(ImmoFetch.our_ops.rdb_get_lock()).strftime('%y%m%d-%H%M%S') + " - Description"
                ImmoFetch.our_ops.rdb_release()
                tmpHistoValue                           = "%s" % tmpAnnonceInDB['description']
                tmpAnnonceInDB['description']           = obj['description']
                tmpAnnonceInDB['description_stz']       = obj['description_stz']
                tmpAnnonceInDB['history'][tmpHistoKey]  = tmpHistoValue
                tmpAnnonceInDB['ts_updated']            = obj['ts_lastfetched']
                retour = 'updated'

            # Nouvelles images ?
            if len(tmpAnnonceInDB['url_images']) != len(obj['url_images']) :
                # -- Ajout des images dans la base
                obj['images_ids'] = obj['images_ids'] + ImmoFetch.push_images_to_rdb(obj)
                tmpHistoKey                      = obj['ts_lastfetched'].run(ImmoFetch.our_ops.rdb_get_lock()).strftime('%y%m%d-%H%M%S') + " - Images"
                ImmoFetch.our_ops.rdb_release()
                tmpHistoValue                           = "%d images" % len(tmpAnnonceInDB['url_images'])
                tmpAnnonceInDB['history'][tmpHistoKey]  = tmpHistoValue
                tmpAnnonceInDB['images_ids'] = ImmoFetch.push_images_to_rdb(obj)
                tmpAnnonceInDB['ts_updated'] = obj['ts_lastfetched']
                retour = 'updated'

            # Update de l'objet dans la db : a minima pour le ts_lastfetched
            try :
                r.table('immoanno').get(obj['id']).replace(tmpAnnonceInDB).run(ImmoFetch.our_ops.rdb_get_lock())
                ImmoFetch.our_ops.rdb_release()
            except Exception as e :
                logging.error("Echec d'update d'annonce dans RDB : %s | %s" % (str(e), tmpAnnonceInDB))
            # if reponse['replaced'] != 1 :
            #     logging.error("Impossible de remplacer un doc dans la db, Id=%s" % obj['id'])
            #     retour[1] = 0
            # elif retour[1] > 0 :
            #     logging.log(logging.INFO-2, "Update in DB : URL=%s" % str(obj['id']))
            # else :
            #     logging.debug("Refresh in DB : Pub=%s Collec=%s Fetch=%s Update=%s URL=%s" % (str(obj['ts_published']), str(obj['ts_collected']), str(obj['ts_lastfetched']), str(obj['ts_updated']), obj['id']))
        return retour

    # ----- Runing the thread
    def run(self):
        # -- Log d'activite
        #ImmoFetch.log_start_of_first_thread()
        t0 = time.perf_counter()

        # -- Init
        liste_urls_queries  = ImmoFetch.our_ops.cfg.get('fetcher').get(self.my_name) or []
        liste_urls_annonces = []
        liste_items_for_rdb = []
        liste_villes_exclure = ImmoFetch.our_ops.cfg.get('fetcher').get('localites_exclure') or []

        # ---------------------- Recup des urls vers toutes les annonces ----------------------
        tmpIndex=0
        for urlq in liste_urls_queries :
            if tmpIndex > 0 : time.sleep(random.randint(2,30))  # Tempo if needed between http requests
            tmpIndex += 1

            logging.debug("%s Query fetch %d|%d = %s" % (self.my_name, tmpIndex, len(liste_urls_queries), urlq))
            liste_urls = list()
            if   self.my_name == 'notair' :
                liste_urls = ImmoFetch.notai_get_urls_annonces_from_query(urlq)
            elif self.my_name == 'lbc' :
                liste_urls = ImmoFetch.lbc_get_urls_annonces_from_query(urlq)
            elif self.my_name == 'zilek' :
                liste_urls = ImmoFetch.zil_get_urls_annonces_from_query(urlq)
            elif self.my_name == 'sudisere' :
                liste_urls = ImmoFetch.sudi_get_urls_annonces_from_query(urlq)
            elif self.my_name == 'briseline' :
                liste_urls = ImmoFetch.brisel_get_urls_annonces_from_query(urlq)
            elif self.my_name == 'prorural' :
                liste_urls = ImmoFetch.prorural_get_urls_annonces_from_query(urlq)
            else :
                logging.error("Identite non reconnue : %s" % urlq)

            if len(liste_urls) == 0 :
                logging.warning("%s | Requete ne renvoie aucune annonce : %s" % (self.my_name, urlq))
            else :
                liste_urls_annonces = liste_urls_annonces + liste_urls
            logging.log(logging.INFO-2, "%s Query fetched %d|%d NB items = %d" % (self.my_name, tmpIndex, len(liste_urls_queries), len(liste_urls)))
        liste_urls_annonces = list(set(liste_urls_annonces))  # remove duplicates
        nb_anno_listed = len(liste_urls_annonces)

        # ---------------------- Parsing de chaque annonce ----------------------
        nb_anno_discarded = tmpIndex = 0
        anno_anomalies = list()
        for urla in liste_urls_annonces :
            if tmpIndex > 0 : time.sleep(random.randint(2,30))  # Tempo if needed between http requests
            tmpIndex += 1

            logging.debug("Annon fetch %d|%d = %s" % (tmpIndex, len(liste_urls_annonces), urla))
            new_item = dict()
            if   self.my_name == 'notair' :
                new_item = ImmoFetch.notai_parse_annonce_from_url(urla)
            elif self.my_name == 'lbc' :
                new_item = ImmoFetch.lbc_parse_annonce_from_url(urla)
            elif self.my_name == 'zilek' :
                new_item = ImmoFetch.zil_parse_annonce_from_url(urla)
            elif self.my_name == 'sudisere' :
                new_item = ImmoFetch.sudi_parse_annonce_from_url(urla)
            elif self.my_name == 'briseline' :
                new_item = ImmoFetch.brisel_parse_annonce_from_url(urla)
            elif self.my_name == 'prorural' :
                new_item = ImmoFetch.prorural_parse_annonce_from_url(urla)
            elif self.my_name == 'seloger' :
                pass
            else :
                logging.error("Identite non reconnue : %s" % urla)
            logging.log(logging.INFO-2, "%s Annon fetched %d|%d : %s" % (self.my_name, tmpIndex, len(liste_urls_annonces), str(new_item)))
            if len(new_item) > 0 :
                if len(liste_villes_exclure) > 0 :
                    # --- Filtrage sur les noms de villes
                    if f4s.strMatchAny(liste_villes_exclure, new_item['localite_stz']) or f4s.strMatchAny(liste_villes_exclure, new_item['codepostal']):
                        nb_anno_discarded += 1
                    # --- Filtrage sur la validite de chaque item
                    elif len("%s%s" % (new_item['title'],new_item['description']))< 10 or len(new_item['url_annonce']) < 5 or ('inconnu' in new_item['localite_stz'] and 'inconnu' in new_item['codepostal']) :
                        logging.error("Article semble incorrect, discarded : %s" % str(new_item))
                        anno_anomalies.append(str(urla))
                    else :
                        liste_items_for_rdb.append(new_item)
            else :
                anno_anomalies.append(str(urla))
                logging.log(logging.INFO-2, 'Annonce en anomalie = %s' % urla)

        nb_anno_retenues = len(liste_items_for_rdb)

        # ---------------------- Pushing items to RDB ----------------------
        nb_anno_inserted_in_db = nb_anno_updated_in_db = 0
        tmpIndex=0
        for anno in liste_items_for_rdb :
            tmpIndex += 1
            resultat = ImmoFetch.push_item_to_rdb(anno)
            if resultat == 'inserted' :
                logging.log(logging.INFO-2, "%s Added in DB %d|%d : %s | %s" % (self.my_name, tmpIndex, len(liste_items_for_rdb), anno['title'], anno['id']))
                nb_anno_inserted_in_db += 1
            elif resultat == 'updated' :
                logging.log(logging.INFO-2, "%s Updated in DB %d|%d : %s | %s" % (self.my_name, tmpIndex, len(liste_items_for_rdb), anno['title'], anno['id']))
                nb_anno_updated_in_db += 1

        # -- Log d'activite
        t1 = time.perf_counter()
        nb_anno_anomaly = nb_anno_listed - nb_anno_discarded - nb_anno_retenues
        logging.info("Site %s : %d sec | %s listed | %d anomalies | %d discarded | %d valid  | %d new inserted | %d updated in db" % (self.my_name, int(t1-t0), nb_anno_listed, nb_anno_anomaly, nb_anno_discarded, nb_anno_retenues, nb_anno_inserted_in_db, nb_anno_updated_in_db))
        llog = dict()
        llog['website']            = self.my_name
        llog['duree_sec']          = int(t1-t0)
        llog['annonces_listed']    = int(nb_anno_listed)
        llog['annonces_anomaly']   = int(nb_anno_anomaly)
        llog['annonces_discarded'] = int(nb_anno_discarded)
        llog['annonces_valid']     = int(nb_anno_retenues)
        llog['annonces_updated']   = int(nb_anno_updated_in_db)
        llog['annonces_new']       = int(nb_anno_inserted_in_db)
        llog['url_anomalies']      = list(anno_anomalies)
        ops.insertLog(typelog="fetching", fields=llog)
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_listed),         tags={'website' : self.my_name, 'number_of' : 'annonces_listed'})
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_anomaly),        tags={'website' : self.my_name, 'number_of' : 'annonces_anomaly'})
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_discarded),      tags={'website' : self.my_name, 'number_of' : 'annonces_discarded'})
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_retenues),       tags={'website' : self.my_name, 'number_of' : 'annonces_valid'})
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_updated_in_db),  tags={'website' : self.my_name, 'number_of' : 'annonces_updated'})
        # self.our_ops.insertKPI(measurement='number', value=int(nb_anno_inserted_in_db), tags={'website' : self.my_name, 'number_of' : 'annonces_new'})

        # twait = 63 - int(t1-t0)  # On calcule le temps a attendre si le process a pris moins de 1 min, pour affichage grafana
        # if twait>1 :
        #     time.sleep(twait)
        # ImmoFetch.log_end_of_last_thread()

    # ------------------------ Notair ------------------------
    @staticmethod
    def notai_get_urls_annonces_from_query(urlq):
        lis_urls = []
        try :
            # -- Extract site info
            u = urllib.request.Request(urlq)
            url_host_full    = u.type + "://" + u.host

            #-- Rendering in headless browser (playing JS)
            ip_splash = ImmoFetch.our_ops.cfg.get('servers').get('splashserver') or '127.0.0.1'
            splash_url = "http://" + ip_splash + ":8050/render.html"
            splash_params = {'url': urlq, 'timeout': 45, 'resource_timeout' : 30, 'images' : 1, 'expand' : 1, 'wait':10 } # images 1 needed
            resp = requests.get(splash_url, params=splash_params, timeout=60)

            #-- Scraping the html page
            soup = BeautifulSoup(resp.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            lis_arefs = soup.find_all("a", class_="annonce_complete") # objet a avec mot cle de classe CSS
            for a in lis_arefs :
                # -- cleaning links
                lis_split_params = str(a.get('href')).split('?')
                url_base         = lis_split_params[0]
                url_params       = lis_split_params[1] if len(lis_split_params)>1 else ''
                lis_params       = url_params.split('&')
                # recup du param necessaire (pas les autres) : 'idAnnonce=123456'
                paramidannonc = ''
                for p in lis_params :
                    if 'idAnnonce' in p :
                        paramidannonc = p
                lis_urls.append(url_host_full + url_base + '?' + paramidannonc)
        except Exception as exp :
            logging.critical('Exception Query Notai : URL = %s | E = %s' % (urlq, str(exp)))

        lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def notai_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            #-- Rendering in headless browser (playing JS)
            ip_splash = ImmoFetch.our_ops.cfg.get('servers').get('splashserver') or '127.0.0.1'
            splash_url = "http://" + ip_splash + ":8050/render.html"
            splash_params = {'url': urla, 'timeout': 45, 'resource_timeout' : 30, 'images' : 1, 'expand' : 1, 'wait':10 }
            resp = requests.get(splash_url, params=splash_params, timeout=60)

            #-- Scraping the html page
            soup = BeautifulSoup(resp.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])

            #-- titre de la forme "type a vendre - ville (CP) - n pieces - s m2"
            a = soup.find_all("h1", class_="titre") # objet a avec mot cle de classe CSS
            atitre = f4s.strCleanSanitize(a[0].text, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=False, pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=False, pLetterDigitTiretOnly=False, pBagOfWords=False).lower()
            atitre = f4s.strMultiReplace([("a vendre", ""), ("maison / villa", "maison")], atitre)
            atitre = re.sub(u" +", " ", atitre).strip()
            titl = atitre.title()
            obj['title'] = titl[:1].capitalize() + titl[1:].lower()
            obj['title_stz'] = f4s.cleanOnlyLetterDigit(obj['title']).lower()
            if 'maison' in obj['title_stz'] :
                obj['type2bien'] = 'maison'
            elif 'terrain' in obj['title_stz'] :
                obj['type2bien'] = 'terrain'

            #-- le prix
            # lis_prix = soup.select("div.ng-scope > div.strong > p")
            lis_prix = soup.select("div.ng-scope > div.strong > p")
            prixtxt = ''
            for i in lis_prix:
                if '€' in i.text :
                    prixtxt = "".join([c for c in i.text if c in string.digits])
                    break
            if len(prixtxt) > 1 :
                obj['price'] = int(prixtxt)

            #-- Commune & code postal
            lis_lieu = soup.select('div.localisation > div.ng-scope > p.ng-binding')
            lieu = departement = ''
            for i in lis_lieu :
                if not('Commune du bien' in i.text) :
                    departement = "".join([c for c in i.text if c in string.digits])
                    validFilenameChars = " %s" % string.ascii_letters
                    lieu1 = "".join([c for c in i.text if c in validFilenameChars])
                    lieu = f4s.cleanMax(lieu1)
                    # lieu = f4s.cleanOnlyLetterDigit(i.text).lower()
            #print('Addr : %s | %s' % (departement, lieu))
            nom_ville = fuzzywuzzy.process.extract(lieu, ImmoFetch.our_villenoms, limit=1)
            obj['codepostal'] = departement
            if len(nom_ville)>0 :
                if len(nom_ville[0])>0 :
                    obj['localite_stz'] = nom_ville[0][0]
                    obj['codepostal'] = ImmoFetch.our_ville2codep.get(nom_ville[0][0]) or departement

            # -- Description
            descrip = soup.select("div.text_description")
            if len(descrip)>0 :
                descr = descrip[0].text
                obj['description'] = descr
                obj['description_stz'] = f4s.strCleanSanitize(descr, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True, pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=True, pLetterDigitTiretOnly=False, pBagOfWords=False)

            #-- topics en liste
            lis_topics = soup.select("div.zone_right > ul > li")
            for tp in lis_topics :
                tp2 = f4s.strCleanSanitize(tp.text, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True, pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=False, pLetterDigitTiretOnly=False, pBagOfWords=False).lower()
                tp3 = tp2.split(':')
                if len(tp3)>1 :
                    if 'habitable' in tp3[0] :
                        valeur = tp3[1].replace('m2', '')
                        valeur = "".join([c for c in valeur if c in string.digits])
                        if valeur.isdigit() :
                            obj['surface'] = int(valeur)
                    elif 'terrain' in tp3[0] :
                        valeur = tp3[1].replace('m2', '')
                        valeur = "".join([c for c in valeur if c in string.digits])
                        if valeur.isdigit() :
                            obj['surface_terrain'] = int(valeur)
                    elif 'de pieces' in tp3[0] :
                        valeur = "".join([c for c in tp3[1] if c in string.digits])
                        if valeur.isdigit() :
                            obj['nbpieces'] = int(valeur)
                    elif 'mise a jour' in tp3[0] :
                        valeur = "".join([c for c in tp3[1] if c in '0123456789/'])
                        tmpDatetime = datetime.strptime(valeur, '%d/%m/%Y')
                        localtz = timezone('Europe/Paris')
                        obj['ts_published'] = r.iso8601(localtz.localize(tmpDatetime).isoformat())

            #-- images
            obj['url_images'] = list()
            #for im in list(soup.select("section div.diaporama div.thumbnail div div div img")) :
            for im in list(soup.find_all(name='img', attrs={"ng-click": True, "ng-src": True, "src": True})) :
                if im.get('src').startswith('http') :
                    obj['url_images'].append(im.get('src'))
            obj['url_images'] = list(set(obj['url_images']))

            #-- uploaded by
            obj['uploadby'] = 'Notaire'

            #-- Sources
            obj['sources'] = ['immonotaire']

            #-- url_annonce
            obj['url_annonce'] = urla
        except Exception as exp :
            logging.error('Exception Annonce Notai : URL = %s | E = %s' % (urla, str(exp)))

        return obj

    # ------------------------ LBC ------------------------
    @staticmethod
    def lbc_get_urls_annonces_from_query(urlq):
        lis_urls = []
        try :
            response = requests.get(urlq, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            # Boucle sur les tags contenant les url vers chaque annonce matchant la requete
            for ref_immo in soup.select('main section section section ul li a') :
                if "www.leboncoin.fr/ventes_immobilieres" in ref_immo.get("href") :
                    tmpAddr = str(ref_immo.get("href"))
                    if tmpAddr.startswith("//") :
                        tmpAddr = "http:" + tmpAddr
                    lis_urls.append(tmpAddr)
        except Exception as exp :
            logging.critical('Exception Query LBC : URL = %s | E = %s' % (urlq, str(exp)))

        lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def lbc_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            response = requests.get(urla, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except:
            logging.error("LBC : Page web non trouvee : %s" % urla)
        else:
            obj['url_annonce'] = urla
            try :
                tmpTitre = soup.select('section[id="adview"] h1[class="no-border"]')[0].decode()
                titl     = f4s.cleanLangueFr(tmpTitre)
                obj['title'] = titl[:1].capitalize() + titl[1:].lower()
                obj['title_stz'] = f4s.cleanOnlyLetterDigit(tmpTitre)
            except :
                logging.warning("LBC : Impossible de caster le titre sur url=%s" % urla)

            tmpPr = soup.select('div[class="line"] h2[itemprop="price"]')
            if len(tmpPr) > 0 :
                tmpPrix = f4s.cleanOnlyLetterDigit(tmpPr[0]['content'])
                try :
                    obj['price'] = int(tmpPrix)
                except :
                    logging.error("LBC : Impossible de caster le prix sur url=%s" % urla)

            tmpListe = soup.find_all("p", attrs={"class":"value", "itemprop": "description"})
            if len(tmpListe) > 0 :
                obj['description']     = f4s.cleanLangueFr(tmpListe[0].decode())
                obj['description_stz'] = f4s.cleanOnlyLetterDigit(tmpListe[0].decode())

            for champs in soup.select('h2[class="clearfix"]') :
                cle = f4s.cleanOnlyLetterDigit(champs.select('span[class="property"]')[0].text).lower()
                try :
                    if "ville" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"]')[0].text)
                        tmpCP = re.findall(r'[0-9][0-9][0-9][0-9][0-9]',valeur)
                        if len(tmpCP)>0 :
                            obj['codepostal'] = str(tmpCP[0])
                        tmpLoc2 = str(valeur).replace(obj['codepostal'], "")
                        obj['localite_stz'] = f4s.cleanMax(tmpLoc2)
                    elif "type de bien" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"]')[0].text)
                        obj['type2bien'] = valeur
                    elif "surface" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"]')[0].text)
                        obj['surface'] = int(valeur[:-3].replace(" ","",-1))   # on enleve le " m2" puis tous les espaces car "1 000" ne caste pas en int, il faut 1000
                    elif "pieces" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"]')[0].text)
                        obj['nbpieces'] = int(valeur)
                    elif "classe energie" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"] a')[0].text)
                        obj['classeenergie'] = valeur
                    elif "ges" in cle :
                        valeur = f4s.cleanOnlyLetterDigit(champs.select('span[class="value"] a')[0].text)
                        obj['ges'] = valeur
                except:
                    if 'ville' in cle or 'type de bien' in cle :
                        logging.warning("LBC : Impossible de caster %s sur url=%s" % (cle, urla))
                    else :
                        logging.debug("LBC : Impossible de caster %s sur url=%s" % (cle, urla))

            obj['uploadby'] = "inconnu"
            tmpListe = soup.find_all("a", attrs={"class":"uppercase bold trackable"})
            if len(tmpListe) > 0 :
                obj['uploadby'] = f4s.cleanOnlyLetterDigit(tmpListe[0].text)

            # --- Les dates
            tmpUploadTime = ""
            tmpListe = soup.find_all("p", attrs={"class":"line line_pro"})
            if len(tmpListe) > 0 :
                try:
                    tmpD = tmpListe[0].contents[0].split(sep='en ligne le ', maxsplit=1)[1]
                    tmpUploadTime = f4s.cleanOnlyLetterDigit(tmpD).lower()
                except:
                    pass

            # --- Parsing de la date Date de la forme : "9 septembre a 14:00."
            tmpNow = datetime.now()
            try :
                tmpUploadTime = f4s.strMultiReplace([('janvier','01'), ('fevrier','02'), ('fvrier','02'), ('mars','03'), ('avril','04'), ('mai','05'), ('juin','06'),
                                                   ('juillet','07'),('aout','08'), ('septembre','09'), ('octobre','10'), ('novembre','11'), ('decembre','12')], tmpUploadTime)
                if len(tmpUploadTime.split(" ")[0]) == 1 :  # Si 1 seul chiffre au debut, on ajoute un "0" devant
                    tmpUploadTime = "0" + tmpUploadTime
                localtz = timezone('Europe/Paris')                                       # On localize la date comme etant en France. Le decalage d'ete ne semble pas pris en compte par contre, juste la timezone
                tmpUploadTime += " " + str(localtz.localize(tmpNow).year)
                tmpDatetime = datetime.strptime(tmpUploadTime, '%d %m a %H %M %Y')      # 9 09 a 14:00."
                obj['ts_published'] = r.iso8601(localtz.localize(tmpDatetime).isoformat()) # creation d'un object serialiable rethinkDB pour ce datetime
            except :
                logging.warning("LBC : Impossible de caster la date : %s" % tmpUploadTime)
                localtz = timezone('Europe/Paris')                                       # On localize la date comme etant en France. Le decalage d'ete ne semble pas pris en compte par contre, juste la timezone
                obj['ts_published'] = r.iso8601(localtz.localize(tmpNow).isoformat())

            # --- Les images
            obj['url_images'] = list()
            for scligne in soup.select('section[class="adview_main"] script') :
                it = re.findall(r'"//.+jpg"', str(scligne))
                for chaine in it :
                    if '/thumbs/' not in chaine and '/ad-thumb/' not in chaine :
                        chaine2 = 'http:' + chaine[1:-1]    # pour enlever les "" autour & ajouter l'url complete
                        if chaine2 not in obj['url_images']:
                            obj['url_images'].append(chaine2)
            obj['url_images'] = list(set(obj['url_images']))

            # -- sources
            obj['sources'] = ['lbc']

        return obj

    # ------------------------ Zil ------------------------
    @staticmethod
    def zil_get_urls_annonces_from_query(urlq):
        lis_urls = list()
        try :
            response = requests.get(urlq, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            liste_references = [i.text for i in soup.select("html > body > div > div > div > div > div > table > tr > td > div > span")]
            liste_url_vrac = [i['href'] for i in soup.select("html > body > div > div > div > div > div > table > tr > td > div > div > a")]
            for ref in liste_references :
                for url_zil in liste_url_vrac :
                    if "/"+ref+".htm" in url_zil :
                        tmpu = "http://zilek.fr/"+url_zil
                        if tmpu not in lis_urls :
                            lis_urls.append(tmpu)
                        break
        except Exception as exp :
            logging.critical('Exception Query ZIL : URL = %s | E = %s' % (urlq, str(exp)))

        lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def zil_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            response = requests.get(urla, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except Exception as e :
            logging.error("ZIL : Page web non trouvee : %s | %s" % (urla, str(e)))
        else :
            # -- TITRE
            obj['url_annonce'] = urla
            try :
                tmpTit = soup.select("html > body > div > div > div > div > div > h1")[0].decode()
                titl         = f4s.cleanLangueFr(tmpTit)
                obj['title'] = titl[:1].capitalize() + titl[1:].lower()
                obj['title_stz'] = f4s.cleanOnlyLetterDigit(tmpTit)
            except :
                logging.warning("Zil : Impossible de caster le titre sur url=%s" % urla)

            # -- plusieurs parsing
            localtz = timezone('Europe/Paris')
            for h3 in soup.select("h3") :
                if 'prix' in str(h3.text).lower() :
                    # --- PARSING PRIX
                    ul = h3.find_next_sibling()
                    try :
                        str_prix = ul.li.contents[0]
                        str_prix = re.sub("\D", "", f4s.cleanOnlyLetterDigit(str_prix))
                        obj['price'] = int(str_prix)
                    except :
                        logging.debug("Zil : Impossible de caster le prix : %s in %s" % (h3,urla))

                    # --- PARSING DATE de la forme : "ajoute au site le 24 Mar 2016" ou " 4 Mar 2016"
                    try :
                        chaine1 = f4s.cleanOnlyLetterDigit(ul.text).lower()
                        chaine2 = f4s.strMultiReplace([('jan','01'), ('fev','02'), ('feb','02'), ('mar','03'), ('avr','04'), ('apr','04'), ('mai','05'), ('may','05'), ('juin','06'), ('jun','06'), ('juil','07'),('jul','07'), ('aou','08'), ('aug','08'), ('sep','09'), ('oct','10'), ('nov','11'), ('dec','12')], chaine1)
                        chaine3 = f4s.strMultiReplace([(' 1 ','01 '), (' 2 ','02 '), (' 3 ','03 '), (' 4 ','04 '), (' 5 ','05 '), (' 6 ','06 '), (' 7 ','07 '), (' 8 ','08 '), (' 9 ','09 ')], chaine2)
                        chaine4 = chaine3[chaine3.find("ajoute au site le")+17:]
                        cha = re.search("[0-9][0-9] [0-9][0-9] [0-9][0-9][0-9][0-9]", chaine4)
                        tmpDatetime = datetime.strptime(cha.group(0), '%d %m %Y')
                        obj['ts_published'] = r.iso8601(localtz.localize(tmpDatetime).isoformat())
                    except :
                        logging.debug("Zil : Impossible de caster la date : %s in %s" % (h3,urla))

                elif 'emplacement' in str(h3.text).lower() :
                    # --- PARSING LOCALITE et CODE POSTAL
                    try :
                        ul = h3.find_next_sibling()
                        m = re.search('commune : (.+) \((.+)\)', ul.li.text)
                        obj['localite_stz'] = f4s.cleanMax(m.group(1))
                        obj['codepostal'] = f4s.cleanOnlyLetterDigit(m.group(2))
                    except :
                        logging.warning("Zil : Impossible de caster localite et code postal : %s in %s" % (h3,urla))

                elif 'description' in str(h3.text).lower() :
                    # --- PARSING DESCRIPTION
                    try :
                        ul = h3.find_next_sibling()
                        obj['description']     = f4s.cleanLangueFr(ul.text)
                        obj['description_stz'] = f4s.cleanOnlyLetterDigit(ul.text)
                    except :
                        logging.debug("Zil : Impossible de caster description : %s in %s" % (h3,urla))

                elif 'le vendeur est' in str(h3.text).lower() :
                    # --- PARSING uploadby
                    try :
                        ul = h3.find_next_sibling()
                        tmpV = str(ul.contents[0].text).replace("@", "a")
                        obj['uploadby'] = f4s.cleanOnlyLetterDigit(tmpV)
                    except :
                        obj['uploadby'] = "Agence"

            # --- TYPE DE BIEN
            obj['type2bien'] = 'inconnu'
            if 'maison' in obj['title_stz'].lower() or 'villa' in obj['title_stz'].lower() :
                obj['type2bien'] = 'maison'
            elif 'terrain' in obj['title_stz'].lower() :
                obj['type2bien'] = 'terrain'
            elif 'appart' in obj['title_stz'].lower() :
                obj['type2bien'] = 'appartement'

            # --- IMAGES # les noms des thumbs et des grandes images sont les memes, seul le chemin change pour avoir la grande taille
            obj['url_images'] = list()
            try :
                lien_img_grand = soup.find('img', attrs={'id':'pic0'})['src']
                m=re.search('http://(.+)/(.+)\.(.+)', lien_img_grand)
                chemin_full_img = 'http://' + m.group(1) + '/'
                for imm in soup.select("html > body > div > div > div > div > div > a > img") :
                    if 'thumbnail' in str(imm.get('alt','')) :
                        m2=re.search('http://(.+)/(.+)\.(.+)', imm.get('src'))
                        tmpUrl = chemin_full_img+m2.group(2)+"."+m2.group(3)
                        obj['url_images'].append(tmpUrl)
            except :
                logging.warning("Zil : Impossible de recup les images for %s" % urla)
            obj['url_images'] = list(set(obj['url_images']))

            # -- sources
            obj['sources'] = ['zilek']

        return obj

    # ------------------------ SUDISER ------------------------
    @staticmethod
    def sudi_get_urls_annonces_from_query(urlq):
        lis_urls = list()
        try :
            response = requests.get(urlq, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            # Boucle sur les tags contenant les url vers chaque annonce matchant la requete
            for ref_immo in soup.select("a.figure") : # for ref_immo in soup.select("div#global > div#center > div#content > div#fiche > div#annonces > div.pane > ul > li > h3 > a") :
                if ref_immo.get('href', '') != '' :
                    tmpURLann = 'http://www.immo-isere.com/' + ref_immo.get('href', '')
                    if tmpURLann not in lis_urls :
                        lis_urls.append(tmpURLann)
        except Exception as exp :
            logging.critical('Exception Query SUDI : URL = %s | E = %s' % (urlq, str(exp)))

        # lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def sudi_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            response = requests.get(urla, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except Exception as e :
            logging.error("SUDI : Page web non trouvee : %s | %s" % (urla, str(e)))
        else :
            obj['url_annonce'] = urla

            try :
                tmpT = soup.select("div section div div span.type")
                if len(tmpT) > 0 :
                    obj['type2bien'] = f4s.cleanOnlyLetterDigit(tmpT[0].text)
            except :
                pass

            try :
                tmpT = soup.select("div section div div span.ville")
                if len(tmpT) > 0 :
                    obj['localite_stz'] = f4s.cleanMax(tmpT[0].text)
                    tmploc = obj['localite_stz'].lower()
                    tmploc2 = f4s.strMultiReplace([('secteur ',''), ('les deux alpes','mont de lans'), ('l alpe d huez', 'huez'), ('trieves', 'lalley'), ('st ','saint '), ('gresse','gresse en vercors')], tmploc)
                    tmploc2 = f4s.strMultiReplace([('cornillon en lalley', 'cornillon en trieves'), ('saint maurice en lalley','saint maurice en trieves'), ('l alpe du grand serre', 'la morte')], tmploc2)
                    if "riouperoux" in tmploc2 :
                        obj['codepostal'] = "38220"
                    else :
                        ville_nom_proche = fuzzywuzzy.process.extract(tmploc2, ImmoFetch.our_villenoms, limit=1)
                        if len(ville_nom_proche) > 0 :
                            obj['codepostal'] = ImmoFetch.our_ville2codep.get(ville_nom_proche[0][0]) or ""
            except Exception as e :
                logging.warning('SUDI : Exception pour ville ou codepostal %s | %s' % (urla, str(e)))

            try :
                tmpT = soup.select("div section div div span.prixannonce")
                if len(tmpT) > 0 :
                    tmpPrix = tmpT[0].text
                    obj['price'] = int("".join([c for c in tmpPrix if c in string.digits]))
            except Exception as e :
                logging.warning('SUDI : Exception pour Prix %s | %s' % (urla, str(e)))

            titl = str(obj['type2bien'] + " " + str(obj['localite_stz']).title() + " " + str(obj['price']) + " €").strip()
            obj['title'] = titl[:1].capitalize() + titl[1:].lower()
            obj['title_stz'] = f4s.cleanOnlyLetterDigit(obj['title'])

            try :
                tmpSurface = soup.select("table tr td.i-surface")[0].text
                obj['surface'] = int("".join([c for c in tmpSurface if c in string.digits]))
            except Exception as e :
                logging.debug('SUDI : Exception pour surface %s | %s' % (urla, str(e)))

            try :
                tmpDesc = f4s.cleanLangueFr(soup.select("div div.desc-contenu")[0].text)
                obj['description'] = f4s.cleanLangueFr(tmpDesc.replace('Description :', ''))
                obj['description_stz'] = f4s.cleanOnlyLetterDigit(obj['description'])
            except Exception as e :
                logging.debug('SUDI : Exception pour description %s | %s' % (urla, str(e)))

            obj['url_images'] = list()
            try :
                tmpImages = soup.select("div#bx-pager a img")
                for img in tmpImages :
                    obj['url_images'].append(str(img['src']).split("?")[0])
            except Exception as e :
                logging.warning('SUDI : Exception pour images %s | %s' % (urla, str(e)))
            obj['url_images'] = list(set(obj['url_images']))

            obj['uploadby'] = "Agence"
            obj['sources'] = ['sudisere']
        return obj

    # ------------------------ BRISEL ------------------------
    @staticmethod
    def brisel_get_urls_annonces_from_query(urlq):
        lis_urls = list()
        try :
            response = requests.get(urlq, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            for ref_immo in soup.select("div h2 a") :
                if ref_immo.get('href', '') != '' :
                    u = urllib.request.Request(urlq)
                    url_host_full = str(u.type + "://" + u.host)
                    tmpRef = ref_immo.get('href', '')
                    tmpURLann = (url_host_full + tmpRef) if tmpRef.startswith('/') else (url_host_full + '/'+ tmpRef)
                    lis_urls.append(tmpURLann)
        except Exception as exp :
            logging.critical('Exception Query BRISEL : URL = %s | E = %s' % (urlq, str(exp)))

        lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def brisel_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            response = requests.get(urla, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except Exception as e :
            logging.error("BRISEL : Page web non trouvee : %s | %s" % (urla, str(e)))
        else :
            obj['url_annonce'] = urla

            try :
                titl = f4s.cleanLangueFr(soup.select("div#page_title h1")[0].text)
                obj['title'] = titl[:1].capitalize() + titl[1:].lower()
                obj['title_stz'] = f4s.cleanOnlyLetterDigit(obj['title']).lower()
            except Exception as e :
                logging.warning('BRISEL : Exception pour Titre %s | %s' % (urla, str(e)))

            try :
                tprix = soup.select("div#size_auto table tr td")[0].text
                obj['price'] = int("".join([c for c in tprix if c in string.digits]))
            except Exception as e :
                logging.warning('BRISEL : Exception pour Prix %s | %s' % (urla, str(e)))

            try :
                obj['description'] = f4s.cleanLangueFr(soup.select("div#details")[0].text)  # on va modifier ce champs plus bas
            except Exception as e :
                logging.debug('BRISEL : Exception pour Description %s | %s' % (urla, str(e)))

            for letr in soup.select('div.overflow_y div.tech_detail tr') :
                if len(letr.select('td')) > 1 :
                    try :
                        mgau = letr.select('td')[0]
                        mdro = letr.select('td')[1]
                        if 'ville' in str(mgau.text).lower() :
                            try :
                                obj['localite_stz'] = f4s.cleanMax(letr.select('span[itemprop="addressLocality"]')[0].text)
                                tmpCP = letr.select('span.acc')[0].text
                                obj['codepostal'] = "".join([c for c in tmpCP if c in string.digits])
                            except Exception as e :
                                logging.warning('BRISEL : Exception pour Ville et CP %s | %s' % (urla, str(e)))
                        elif 'type' in str(mgau.text).lower() :
                            if f4s.strMatchAny(['individ', 'jumele', 'mitoy'], mdro.text) :
                                obj['type2bien'] = 'maison'
                        elif 'surface' in str(mgau.text).lower() and not('surface au sol' in str(mgau.text).lower())  :
                            tmpS = f4s.strCleanSanitize(mdro.text, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=False, pLetterDigitTiretOnly=False, pBagOfWords=False)
                            tmpS = tmpS.replace('m2','')
                            if '.' in tmpS :
                                tmpS = str(mdro.text).split('.')[0]
                            elif ',' in tmpS :
                                tmpS = str(mdro.text).split(',')[0]
                            obj['surface'] = int("".join([c for c in tmpS if c in string.digits]))
                        elif 'terrain' in str(mgau.text).lower() :
                            tmpS = f4s.strCleanSanitize(mdro.text, phtmlunescape=True, pLignesTabsGuillemets=True, pNormalizeASCII=True, pEnleveSignesSpeciaux=True, pLettreDigitPointTiret=False, pLetterDigitTiretOnly=False, pBagOfWords=False)
                            tmpS = tmpS.replace('m2','')
                            if '.' in tmpS :
                                tmpS = str(mdro.text).split('.')[0]
                            elif ',' in tmpS :
                                tmpS = str(mdro.text).split(',')[0]
                            obj['surface_terrain'] = int("".join([c for c in tmpS if c in string.digits]))
                        elif 'pieces' in f4s.cleanMax(mgau.text).lower() :
                            obj['nbpieces'] = int("".join([c for c in f4s.cleanMax(mdro.text) if c in string.digits]))
                        elif 'etat general' in f4s.cleanMax(mgau.text).lower() :
                            obj['description'] += " " + f4s.cleanLangueFr(mdro.text)
                    except Exception as e :
                        logging.debug('BRISEL : Exception dans champs %s | %s' % (urla, str(e)))

            obj['description_stz'] = f4s.cleanOnlyLetterDigit(obj['description'])  # la desc peut etre modifiee avec les champs

            #-- images
            obj['url_images'] = list()
            try :
                for im in list(soup.select("div#page_content div[itemscope] div#layerslider a img[src]")) :
                    if im.get('src').startswith('http') :
                        obj['url_images'].append(str(im.get('src')).split("?")[0])
            except Exception as e :
                logging.warning('BRISEL : Exception dans images %s | %s' % (urla, str(e)))
            obj['url_images'] = list(set(obj['url_images']))

            obj['uploadby'] = "Agence"
            obj['sources'] = ['briseline']
        return obj

    # ------------------------ PRORURAL ------------------------
    @staticmethod
    def prorural_get_urls_annonces_from_query(urlq):
        lis_urls = list()
        try :
            response = requests.get(urlq, timeout=30)
            soup = BeautifulSoup(response.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
            for ref_immo in soup.select("div.conteneur_liste_auto div.detail_offre_ligneload div.img_offre a[href]") :
                if ref_immo.get('href', '') != '' :
                    u = urllib.request.Request(urlq)
                    url_host_full = str(u.type + "://" + u.host) + "/fr"  # Mano
                    tmpRef = ref_immo.get('href', '')
                    tmpURLann = (url_host_full + tmpRef) if tmpRef.startswith('/') else (url_host_full + '/'+ tmpRef)
                    lis_urls.append(tmpURLann)
        except Exception as exp :
            logging.critical('Exception Query PRORURAL : URL = %s | E = %s' % (urlq, str(exp)))

        lis_urls = list(set(lis_urls))  # remove duplicates
        return lis_urls
    @staticmethod
    def prorural_parse_annonce_from_url(urla):
        obj = ImmoFetch.get_empty_obj()
        try :
            #-- Rendering in headless browser (playing JS)
            ip_splash = ImmoFetch.our_ops.cfg.get('servers').get('splashserver') or '127.0.0.1'
            splash_url = "http://" + ip_splash + ":8050/render.html"
            splash_params = {'url': urla, 'timeout': 45, 'resource_timeout' : 30, 'images' : 1, 'expand' : 1, 'wait':10 }
            resp = requests.get(splash_url, params=splash_params, timeout=60)
            #-- Scraping the html page
            soup = BeautifulSoup(resp.content, "html.parser", from_encoding='utf-8', exclude_encodings=["iso-8859-2"])
        except Exception as e :
            logging.error("PRORURAL : Page web non trouvee : %s | %s" % (urla, str(e)))
        else :
            obj['url_annonce'] = urla
            try :
                titl = f4s.cleanLangueFr(soup.select("div.conteneurGeneral div.colonne_droite div h1")[0].text)
                obj['title'] = titl[:1].capitalize() + titl[1:].lower()
                obj['title_stz'] = f4s.cleanOnlyLetterDigit(obj['title']).lower()
            except Exception as e :
                logging.warning('PRORURAL : Exception pour Titre %s | %s' % (urla, str(e)))

            try :
                tprix = soup.select("div.conteneurGeneral div.colonne_droite div.infoPrincipale span.libChpOffre.prix span.valChpOffre.prix")[0].text
                obj['price'] = int("".join([c for c in tprix if c in string.digits]))
            except Exception as e :
                logging.warning('PRORURAL : Exception pour Prix %s | %s' % (urla, str(e)))

            bclSituationDone=False
            for letr in soup.select("div.conteneurGeneral div.colonne_droite div.infoPrincipale span.libChpOffre") :
                try :
                    gauc = f4s.cleanMax(letr.get_text().split(":")[0])
                    droi = letr.select("span")[0].get_text()
                    if 'situation' in gauc :
                        # besoin parser une explication, pas de champs
                        tmpLieu = f4s.cleanMax(droi)
                        tmpL2 = "".join([c for c in tmpLieu if c not in string.digits])
                        bow = tmpL2.split(" ")
                        bow2 = [c for c in bow if len(c)>4]
                        for mot in bow2 :
                            if mot in ImmoFetch.our_villenoms :
                                obj['localite_stz'] = mot
                                obj['codepostal'] = ImmoFetch.our_ville2codep[mot]
                                bclSituationDone = True
                                break
                    elif 'departement' in gauc :
                        if not bclSituationDone :
                            # -- lieu pas complete, on met au moins le dept
                            dept = f4s.cleanMax(droi)
                            if f4s.strMatchAny(['isere', '38'], dept) :
                                obj['localite_stz'] = 'isere'
                                obj['codepostal'] = '38'
                            elif f4s.strMatchAny(['savoie', '73'], dept) :
                                obj['localite_stz'] = 'savoie'
                                obj['codepostal'] = '73'
                            elif f4s.strMatchAny(['ardeche', '07'], dept) :
                                obj['localite_stz'] = 'ardeche'
                                obj['codepostal'] = '07'
                            elif f4s.strMatchAny(['ain', '01'], dept) :
                                obj['localite_stz'] = 'ain'
                                obj['codepostal'] = '01'
                    elif 'description' in gauc :
                        obj['description'] += " " + f4s.cleanLangueFr(droi)
                    elif 'habitation' in gauc :
                        obj['description'] += " " + f4s.cleanLangueFr(droi)
                    elif 'exploitation' in gauc :
                        obj['description'] += " " + f4s.cleanLangueFr(droi)
                    elif 'divers' in gauc :
                        obj['description'] += " " + f4s.cleanLangueFr(droi)
                except Exception as e :
                    logging.debug('PRORURAL : Exception dans champs %s | %s' % (urla, str(e)))

            obj['description'] = f4s.cleanLangueFr(obj['description'])
            obj['description_stz'] = f4s.cleanOnlyLetterDigit(obj['description'])

            #-- images
            obj['url_images'] = list()
            try :
                for im in list(soup.select("img[rel]")) :
                    if im.get('rel').startswith('http') :
                        obj['url_images'].append(str(im.get('rel')))
                obj['url_images'] = list(set(obj['url_images']))
            except Exception as e :
                logging.warning('PRORURAL : Exception dans images %s | %s' % (urla, str(e)))
            obj['url_images'] = list(set(obj['url_images']))

            obj['uploadby'] = "Agence"
            obj['sources'] = ['proprurales']
        return obj

def launch_fetcher(nom_in_config, ops_ptr, wait_max_random=1800):
    ImmoFetch.init_static(ops_ptr)
    time.sleep(random.randint(1,wait_max_random))
    tmpObj = ImmoFetch(nom_in_config)
    tmpObj.start()

if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.addLevelName(logging.INFO-2, 'INFO2') # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule
    logging.info("Starting from %s" % str(os.getcwd()))

    ops = Ops4app.get_instance(appli_name="immfetch", config_default_file_path='../immtrk.config.defaults.toml')
    nblaunch = 0
    daily_hour = ops.cfg.get('fetcher').get('daily_hour') or ""  # Format hh:mm
    if ops :
        for nom in (ops.cfg.get('fetcher') or dict()).keys() :
            if type(ops.cfg.get('fetcher')[nom]) is type([]) :
                if nom in ['prorural', 'briseline', 'notair', 'lbc', 'zilek', 'sudisere'] :
                    if len(daily_hour) != 5 :  # Format hh:mm
                        logging.info("Lancement unique")
                        launch_fetcher(nom_in_config=nom, ops_ptr=ops, wait_max_random=20)
                    else :
                        logging.info("Daily launch around %s" % daily_hour)
                        schedule.every(1).day.at(daily_hour).do(launch_fetcher, nom_in_config=nom, ops_ptr=ops, wait_max_random=60) # seront lances a meme heure chaque jour, mais avec tempo random dans la fonction
                        nblaunch += 1

    if nblaunch>0 :
        while True:
            schedule.run_pending()
            time.sleep(10)

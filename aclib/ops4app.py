# coding: utf8
import rethinkdb as r, logging, time, threading, pytoml
from influxdb import InfluxDBClient
from datetime import datetime
from pytz import timezone
from pathlib import Path

# --- RDB en mode exclu (1 instance de ops par thread : 1 conn rdb ) ou avec des lock/release car 1 ptr rdb ne se partage pas quand il est en cours de query
class Ops4app :

    # --- return a new dict with all new fields of all sections inserted (not updated)
    @staticmethod
    def recursive_merge_confs(dico1=None, dico2=None, update=False):
        if dico1 is None and dico2 is None :
            return dict()
        elif dico1 is None:
            return dict(dico2)
        elif dico2 is None :
            return dict(dico1)
        else :
            retour = dict(dico1)                                                # Le retour est base sur le dico1
            cles_dico1 = dico1.keys()
            cles_dico2 = dico2.keys()
            for cle2 in cles_dico2 :
                if cle2 not in cles_dico1 :                                     # l'item de 2 n'est pas dans 1 : on INSERT
                    retour[cle2] = dico2[cle2]
                elif type(dico1[cle2]) is list and type(dico2[cle2]) is list :  # meme type & LIST : merge et remove duplicates
                    retour[cle2] = list(set(dico1[cle2]+dico2[cle2]))
                elif type(dico1[cle2]) is dict and type(dico2[cle2]) is dict :  # meme type et DICT : recursivite
                    retour[cle2] = Ops4app.recursive_merge_confs(dico1[cle2], dico2[cle2], update=update)
                else :                                                          # Autres types ou 2 item de types differents : on laisse ou update
                    if not update : retour[cle2] = dico1[cle2]
                    else :          retour[cle2] = dico2[cle2]
            return retour

    # --- Constructor to be called by the static method
    def __init__(self, appli_name="noname", conf_default=None, conf_local=None, updatefromrdb=False, local_path=Path.cwd()) :
        self._mylocaltz = timezone('Europe/Paris')
        self._my_app_name = appli_name
        self._my_loc_path = local_path

        # ----- OVERRIDE : Config locale override config default
        self._my_config = Ops4app.recursive_merge_confs(conf_default, conf_local, update=True)

        # -- Attributs de base : RDB depuis le fichier
        self._my_rdb                    = None
        self._my_rdb_lock               = threading.Lock()
        self._isOK                      = True
        self._my_rdb_IP                 = self._my_config['servers']['rdb.ip']
        self._my_rdb_port               = self._my_config['servers']['rdb.port']
        self._my_rdb_base               = self._my_config['servers']['rdb.base']
        self._my_config_table_in_rdb    = self._my_config['servers']['rdb.cfg.tbl']

        # -- Recup de la config depuis RDB : transforme chaque objet de rdb en une section dans le dict() de l'instance (equivalent a l'objet toml)
        conf_from_rdb = dict()
        if self.rdb is not None:
            try :
                cfgitems = r.table(self._my_config_table_in_rdb).limit(500).run(self.rdb)
                for cfgitem in cfgitems :
                    new_section = dict(cfgitem)
                    new_section.pop('id')
                    sectfinale = dict()
                    sectfinale.update(self._my_config.get(cfgitem['id']) or {})
                    sectfinale.update(new_section)  # si la section existe,  on merge le contenu
                    conf_from_rdb.update({ cfgitem['id'] : sectfinale })
            except Exception as e :
                logging.warning("No config found for %s | %s" % (appli_name, str(e)))
                self._isOK = True

        # ----- OVERRIDE : Config RDB override config default+local
        self._my_config = Ops4app.recursive_merge_confs(self._my_config, conf_from_rdb, update=updatefromrdb)

        # -- Config IDB
        self._my_idb_IP             = self._my_config['servers']['idb.ip']
        self._my_idb_port           = self._my_config['servers']['idb.port']
        self._my_idb_log            = self._my_config['servers']['idb.login']
        self._my_idb_pwd            = self._my_config['servers']['idb.pwd']
        self._my_kpi_db_in_idb      = self._my_config['servers']['idb.kpi.db']

    # --- Static pour recup l'instance : if None error...
    @staticmethod
    def get_instance(appli_name="", cfg_filename="config.toml", cfg_local_ext="-local", db_replace_conf_file=True) :
        app_path      = Path.cwd()
        app_name      = str(Path.cwd().parts[-1])
        app_path_loc  = Path.cwd().parent / Path(app_name+cfg_local_ext)
        if not app_path_loc.exists() :
            logging.warning('Local directory [%s] not found' % app_path_loc.as_posix())
            app_path_loc = app_path
        my_conf_file  = app_path / Path(cfg_filename)
        my_conf2_file = app_path_loc / Path(cfg_filename)

        if not my_conf_file.exists() and not my_conf2_file.exists() :
            logging.critical('No config files found : %s | %s' % (my_conf_file.as_posix(), my_conf2_file.as_posix()))
            return None

        conf_default = None
        if my_conf_file.exists() :
            try :
                conf_fobj = my_conf_file.open(mode='r', encoding='utf-8', errors='backslashreplace')
                conf_default = pytoml.load(conf_fobj)
                conf_fobj.close()
            except Exception as e :
                logging.error('Cannot parse config file %s' % str(e))
        conf_local = None
        if app_path != app_path_loc and my_conf2_file.exists() :
            try :
                conf_fobj = my_conf2_file.open(mode='r', encoding='utf-8', errors='backslashreplace')
                conf_local = pytoml.load(conf_fobj)
                conf_fobj.close()
            except Exception as e :
                logging.error('Cannot parse config file %s' % str(e))

        if conf_default is None and conf_local is None :
            logging.critical('No config files parsed : %s | %s' % (my_conf_file.as_posix(), my_conf2_file.as_posix()))
            return None

        if len(appli_name) < 1 :
            appli_name = app_name
        the_instance = Ops4app(appli_name, conf_default=conf_default, conf_local=conf_local, updatefromrdb=db_replace_conf_file, local_path=app_path_loc)
        if the_instance.isOK() :
            return the_instance
        else :
            return None
    # --- Return true is instance is correctly created (access to rdb tested)
    def isOK(self):
        return self._isOK
    def getLocalPath(self):
        return self._my_loc_path

    # --- CONFIG Recuperation de la config depuis rethinkDB : dans le pire des cas, liste vide
    @property
    def cfg(self):
        # TODO : Faire une relecture periodique de la config depuis rethinkDB
        return self._my_config
    @cfg.setter
    def cfg(self, p):
        pass  # on ne fait rien en ecriture
    @cfg.deleter
    def cfg(self):
        self._my_config = None  # Cela forcera un reload a la prochaine demande en acces lecture

    # --- RETHINKDB multithread / multi instances : on take et on release (avec attente dans le take si besoin)
    def rdb_get_lock(self):
        if self.rdb is not None :
            self._my_rdb_lock.acquire(blocking=True,timeout=60)
        return self.rdb
    def rdb_release(self):
        self._my_rdb_lock.release()
    # --- RETHINKDB monothread : 1 instance par thread, PERSISTANCE DE LA CONNEXION DONC pas possible d'avoir connexion en thread safe Acces a la DB en get / set / delete
    @property
    def rdb(self):
        #with self._my_rdb_MT_lock :
        if self._my_rdb is None :
            nb_reconnect = 3
            while nb_reconnect > 0 :
                try :
                    self._my_rdb = r.connect(host=self._my_rdb_IP, port=self._my_rdb_port, db=self._my_rdb_base, auth_key="", timeout=10)
                    self._my_rdb.use(self._my_rdb_base)
                    nb_reconnect = 0
                    self._isOK = True
                except Exception as e :
                    self._my_rdb = None
                    self._isOK = False
                    nb_reconnect -= 1
                    if nb_reconnect > 0 :
                        logging.error("Echec connexion a RDB, sleeping before rety : %s" % str(e))
                        time.sleep(30)
        if self._my_rdb is None :
            logging.critical("No connection established to RDB")
        return self._my_rdb
    @rdb.setter  # Acces a la db en set : dans tous les cas, on referme la connexion et on met None -> appel du delete
    def rdb(self, p):
        # with self._my_rdb_MT_lock :
        logging.warning("Tentative d'affecter une valeur au pointeur RDB : %s" % type(p))
        del self.rdb  # logging.log(logging.DEBUG-2, "Deconnexion de la DB via assignement=%s" % str(type(p)) )
    @rdb.deleter  # On referme la connexion a la db
    def rdb(self):
        # with self._my_rdb_MT_lock :
        if self._my_rdb is not None :
            try :
                self._my_rdb.close()
            except Exception as e :
                logging.warning("Erreur durant deconnexion RDB : %s" % str(e))
            self._my_rdb = None

    # --- RETHINKDB : log table
    def insertLog(self, typelog="", fields=None):
        if fields is None :
            fields = dict()
        obj = dict(fields)
        obj['type'] = typelog
        obj['ts_creation'] = r.iso8601(self._mylocaltz.localize(datetime.now()).isoformat())
        if self.rdb_get_lock() :
            r.table('logs').insert(obj).run(self.rdb)
            self.rdb_release()
    # --- INFLUXDB : Injection dans InfluxDB tags de la forme dict({ 'cle' : 'valeur' }), c'est du REST, pas de persistance
    def insertKPI(self, measurement, value, tags=None):
        idbsjon = dict()
        idbsjon['time'] = self._mylocaltz.localize(datetime.now()).isoformat()
        idbsjon['measurement'] = measurement
        idbsjon['fields'] = dict()
        idbsjon['fields']['value'] = value
        idbsjon['tags'] = dict() if tags is None else dict(tags)
        idbsjon['tags']['app_uname'] = self._my_app_name
        try :
            client = InfluxDBClient(host=self._my_idb_IP, port=self._my_idb_port, username=self._my_idb_log, password=self._my_idb_pwd, database=self._my_kpi_db_in_idb, timeout=20)
            client.write_points(list([idbsjon]))
        except Exception as e :
            logging.error("Echec insert KPI dans IDB : %s" % str(e))
    # --- INFLUXDB : Injection custom dans InfluxDB
    def insertInIDB(self, db_name='', liste_objets=None):
        if liste_objets is not None :
            try :
                client = InfluxDBClient(host=self._my_idb_IP, port=self._my_idb_port, username=self._my_idb_log, password=self._my_idb_pwd, database=db_name, timeout=20)
                client.write_points(database=db_name, points=liste_objets)
            except Exception as e :
                logging.error("Echec insert massif dans IDB : %s" % str(e))

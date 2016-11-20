# coding: utf8
import rethinkdb as r, logging, time, threading, pytoml
from influxdb import InfluxDBClient
from datetime import datetime
from pytz import timezone
from pathlib import Path

# --- RDB en mode exclu (1 instance de ops par thread : 1 conn rdb ) ou avec des lock/release car 1 ptr rdb ne se partage pas quand il est en cours de query
class Ops4app :
    # --- Constructor to be called by the static method
    def __init__(self, appli_name="noname", config_default=None) :
        self._mylocaltz = timezone('Europe/Paris')
        self._my_app_name = appli_name
        if config_default is None:
            config_default = {}
        self._my_rdb                = None
        self._my_rdb_lock           = threading.Lock()
        self._my_config             = dict(config_default)
        self._isOK                  = True

        # -- Config RDB : seule compte la valeur du fichier
        self._my_rdb_IP                 = config_default['servers']['rdb.ip']
        self._my_rdb_port               = config_default['servers']['rdb.port']
        self._my_rdb_base               = config_default['servers']['rdb.base']
        self._my_config_table_in_rdb    = config_default['servers']['rdb.cfg.tbl']

        # -- Connexion a RDB pour la config : transforme chaque objet de rdb en une section dans le dict() de l'instance (equivalent a l'objet toml)
        if self.rdb is not None:
            try :
                cfgitems = r.table(self._my_config_table_in_rdb).limit(500).run(self.rdb)
                for cfgitem in cfgitems :
                    new_section = dict(cfgitem)
                    new_section.pop('id')
                    sectfinale = dict()
                    sectfinale.update(self._my_config.get(cfgitem['id']) or {})
                    sectfinale.update(new_section)  # si la section existe,  on merge le contenu
                    self._my_config.update({ cfgitem['id'] : sectfinale })
                # -- Config IDB
                self._my_idb_IP             = self._my_config['servers']['idb.ip']
                self._my_idb_port           = self._my_config['servers']['idb.port']
                self._my_idb_log            = self._my_config['servers']['idb.login']
                self._my_idb_pwd            = self._my_config['servers']['idb.pwd']
                self._my_kpi_db_in_idb      = self._my_config['servers']['idb.kpi.db']

            except Exception as e :
                logging.critical("Problem d'acces a RDB pour config : %s" % str(e))
                self._isOK = False
    # --- Static pour recup l'instance : if None error...
    @staticmethod
    def get_instance(appli_name="noname", config_default_file_path=None) :
        config_default = {}
        if config_default_file_path is not None :
            conf_path = Path(config_default_file_path)
            if not conf_path.exists() :
                logging.error('Default config file not found : %s' % str(config_default_file_path))
            else :
                try :
                    fobj = conf_path.open(mode='r', encoding='utf-8', errors='backslashreplace')
                    config_default = pytoml.load(fobj)
                    fobj.close()
                except Exception as e :
                    logging.error('Default config file %s cannot be interpreted : %s' % (config_default_file_path, (str(e))))

        the_instance = Ops4app(appli_name, config_default)
        if the_instance.isOK() :
            return the_instance
        else :
            return None
    # --- Return true is instance is correctly created (access to rdb tested)
    def isOK(self):
        return self._isOK

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
                    logging.error("Echec connexion a RDB : %s" % str(e))
                    self._my_rdb = None
                    self._isOK = False
                    nb_reconnect -= 1
                    if nb_reconnect > 0 :
                        logging.error("Sleep avant reconnexion RDB")
                        time.sleep(20)
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

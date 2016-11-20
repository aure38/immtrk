import rethinkdb as r, logging, os, sys, csv, pytoml
from pathlib import Path

if __name__ == '__main__':
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS')  # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.addLevelName(logging.INFO-2, 'INFO2')
    logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING) # On desactive les logs pour la librairie requests
    logging.getLogger("schedule").setLevel(logging.WARNING) # On desactive les logs pour la librairie schedule

    print("\n------------------ Directories ------------------")
    print("Repertoire courant = %s " % str(os.getcwd()))
    # sys.path.append('./')
    print("Path Python = %s " % str(sys.path))

    print("\n------------------ Config default ------------------")
    my_conf_file = "./immtrk.config.defaults.toml"
    conf_path = Path(my_conf_file)
    conf_toml = dict()
    if not conf_path.exists() :
        print('ERROR : NO CONFIG FILE FOUND')
        exit(0)
    else :
        conf_fobj = conf_path.open(mode='r', encoding='utf-8', errors='backslashreplace')
        conf_toml = pytoml.load(conf_fobj)
        conf_fobj.close()
        print('Config file found')

    print("\n------------------ Checking RethinkDB server ------------------")
    my_rdb = None
    my_rdb = r.connect(host=conf_toml['servers']['rdb.ip'], port=conf_toml['servers']['rdb.port'], db=conf_toml['servers']['rdb.base'], auth_key="", timeout=10)
    print("Connected to RethinkDB : %s" % conf_toml['servers']['rdb.ip'])

    if conf_toml['servers']['rdb.base'] not in r.db_list().run(my_rdb) :
        print("Base [%s] not found -> Creation" % conf_toml['servers']['rdb.base'])
        r.db_create(conf_toml['servers']['rdb.base']).run(my_rdb)
    else :
        print("Base [%s] found" % conf_toml['servers']['rdb.base'])
    my_rdb.use(conf_toml['servers']['rdb.base'])

    liste_tables_in_db = r.db(conf_toml['servers']['rdb.base']).table_list().run(my_rdb)
    for tmpT in conf_toml['servers']['rdb.tables'] :
        if tmpT not in liste_tables_in_db :
            print("Table [%s] not found -> Creation" % tmpT)
            tmpRep = r.db(conf_toml['servers']['rdb.base']).table_create(tmpT).run(my_rdb)
            if tmpRep.get("tables_created", 0) != 1 :
                print('ERROR : IMPOSSIBLE TO CREATE TABLE %s'% tmpT)
                exit(0)
    print("Tables %s found" % str(r.table_list().run(my_rdb)))

    print("\n------------------ Config override with user defined ------------------")
    my_conf2_file  = "../immtrk-ress/immtrk.config.override.toml"
    my_conf_table = "config"
    conf2_path = Path(my_conf2_file)
    if not conf2_path.exists() :
        print('WARNING : Dump file [%s] not found' % my_conf2_file)
    else :
        print('  - Clearing table [%s] in RDB' % r.table(my_conf_table))
        r.table(my_conf_table).delete().run(my_rdb)
        print('Dumping [%s] to [%s]' % (my_conf2_file, my_conf_table))
        conf2_fobj = conf2_path.open(mode='r', encoding='utf-8', errors='backslashreplace')
        conf2_toml = pytoml.load(conf2_fobj)
        conf2_fobj.close()
        for nom in conf2_toml.keys() :
            if type(conf2_toml[nom]) is type({}) :
                conf_item = dict(conf2_toml[nom])
                conf_item['id'] = nom
                try :
                    res = r.table(my_conf_table).insert(conf_item, conflict="update", return_changes=False).run(my_rdb)
                    if res['inserted'] == 1 :
                        print("  - Created : [%s] = %s" % (nom, conf2_toml[nom]))
                    elif res['replaced'] == 1 :
                        print("  - Updated : [%s] = %s" % (nom, conf2_toml[nom]))
                    elif res['unchanged'] == 1 :
                        print("  - Unchanged : [%s] = %s" % (nom, conf2_toml[nom]))
                    else :
                        print("  - ERROR with [%s] : res=%s" % (nom, str(res)))
                except Exception as e:
                    print("Exception with %s : %s" % (nom, str(e)))
                    exit(0)

    print("\n------------------ Dump : Table ressource from csv ------------------")
    my_dump_file  = "../immtrk-ress/immotrack-villes-fr.csv"
    my_dump_table = "villesfr"
    dump_path = Path(my_dump_file)
    if not dump_path.exists() :
        print('WARNING : File [%s] not found' % my_dump_file)
    else :
        print('Dumping [%s] to [%s]' % (my_dump_file, my_dump_table))
        print('  - Clearing table [%s] in RDB' % r.table(my_dump_table))
        r.table(my_dump_table).delete().run(my_rdb)
        print('  - Reading file')
        villes_rdb = list()
        dump_fobj = dump_path.open(mode='r', encoding='utf-8', errors='replace')
        pointeur = csv.reader(dump_fobj, delimiter=',', quotechar='"', doublequote=True, escapechar=None, skipinitialspace=True, quoting=csv.QUOTE_ALL)
        for row in pointeur :
            # 0-5 : Id | NumDepartement | NomMinusculeTiretUnique? | NomMajTirets | NomEspacesPasAccentsPasTirets | NomAvecAccentsTirets |
            # 6-18 : ? | CodeUnique? | CodePOSTAL | "01190" | "284" | "01284" | "2" | "26" | "6" | "618" | "469" | "500" | "93" | "6.6" |
            # 19- : LONGITUDE | LATITUDE | "2866" | "51546" | "+45456" | "462330" | "170" | "205"
            new_ville = dict()
            new_ville['departement'] = row[1].strip()
            try :
                new_ville['point.gps'] = r.point(float(row[19].strip()),float(row[20].strip()))  # long,lati
            except :
                new_ville['point.gps'] = r.point(0.0,0.0)
            new_ville['nom_stz'] = row[4].strip()
            new_ville['nom'] = row[5].strip()
            new_ville['codepostal'] = str(row[8].strip())
            villes_rdb.append(new_ville)
        print('  - Collected %d lines' % len(villes_rdb))
        dump_fobj.close()
        print('  - Injection in %s' % my_dump_table)
        #res = r.db(my_rdb_base).table(my_table_ress_villes_fr).delete().run(my_rdb)
        res = r.table(my_dump_table).insert(villes_rdb, conflict="replace", return_changes=False).run(my_rdb)
        print('  - Inserted|Replaced|Unchanged %d|%d|%d objects' % (res['inserted'], res['replaced'], res['unchanged']))

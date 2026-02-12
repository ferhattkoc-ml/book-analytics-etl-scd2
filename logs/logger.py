import yaml
from sqlalchemy import create_engine, text
from datetime import datetime #Etl run başlangıç ve bitiş zamanlarını almak için kullanılır 

def get_engine():
    with open("config/db_config.yaml","r") as f:    #Database bilgilerini okur
        config = yaml.safe_load(f)
    return create_engine(
        f'mysql+mysqlconnector://{config["user"]}:{config["password"]}@' #MySQL baglantısı oluşturur
        f'{config["host"]}:{config["port"]}/{config["database"]}'
    )    

def log_run_start(pipeline_name:str) -> int:
    engine = get_engine() #DB bağlantısı alınır 
    start_time = datetime.now() #ETL başladıgı zaman
    #Transaction içinde insert yapılır 
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """ INSERT INTO etl_run_log(pipeline_name,start_time ,status)
                VALUES (:p , :st , 'RUNNING')
           """ ),
           {
            "p":pipeline_name, #Pipeline adı
            "st":start_time  #Başlangıç Zamanı 
           }
        )
        run_id = res.lastrowid 
    return run_id     

def log_run_end(
    run_id: int,
    status: str,
    extract_rows: int = None,
    transform_rows: int = None,
    scd_new: int = None,
    scd_changed: int = None,
    scd_inserted: int = None,
    fail_stage: str = None,
    error_message: str = None
):
    # ETL çalışmasının bitiş durumunu, metriklerini ve varsa hata bilgisini log tablosuna yazar
    engine = get_engine()
    end_time = datetime.now()

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_run_log
                SET end_time = :et,
                    duraction_sec = TIMESTAMPDIFF(SECOND, start_time, :et),
                    status = :status,
                    fail_stage = :fs,
                    error_message = :em,
                    extract_rows = :er,
                    transform_rows = :tr,
                    scd_new_count = :sn,
                    scd_changed_count = :sc,
                    scd_inserted_count = :si
                WHERE run_id = :rid
            """),
            {
                "et": end_time,
                "status": status,
                "fs": fail_stage,
                "em": error_message,
                "er": extract_rows,
                "tr": transform_rows,
                "sn": scd_new,
                "sc": scd_changed,
                "si": scd_inserted,
                "rid": run_id,
            }
        )

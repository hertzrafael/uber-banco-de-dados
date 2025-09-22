from source.extract import Extract
from source.transform import Transform
from source.load import Load
import os
from dotenv import load_dotenv

def main():
    extract = Extract()
    dataframe = extract.run()

    transform = Transform(
        frame=dataframe,
        reason_columns=['Reason for cancelling by Customer', 'Incomplete Rides Reason'],
        payment_columns=['Payment Method'],
        status_columns=['Booking Status'],
        car_model_columns=['Vehicle Type']
    )

    load = Load()

    #db_path = os.getenv("DB_PATH")

    #conection = f"sqlite:///{db_path}"

    #for name, frame in transform.init_transform():
    #    load.upload_frame(conection, frame, name)
    
    db_path = os.path.join("C:/Users/Dogec/Documents", "banco de dados", "uber.db")

    for name, frame in transform.init_transform():
        load.create_table_from_dataframe(frame, db_path, name)


if __name__ == '__main__':
    main()
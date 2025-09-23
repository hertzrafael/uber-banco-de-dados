from source.extract import Extract
from source.transform import Transform
from source.load import Load
import os
from dotenv import load_dotenv

def main():
    extract = Extract()
    dataframe = extract.run()
    print(dataframe.shape)

    transform = Transform(
        frame=dataframe,
        reason_columns=['Reason for cancelling by Customer', 'Incomplete Rides Reason', 'Driver Cancellation Reason'],
        payment_columns=['Payment Method'],
        status_columns=['Booking Status'],
        car_model_columns=['Vehicle Type'],
        incomplete_columns=['Incomplete Rides Reason'],
    )
    
    transform.init_transform()

    #for name, frame in transform.init_transform():
    #    print(frame.head(5))
    #    print('-'*20)

    #load = Load()

    #db_path = os.getenv("DB_PATH")

    #conection = f"sqlite:///{db_path}"

    #for name, frame in transform.init_transform():
    #    load.upload_frame(conection, frame, name)
    
    #db_path = os.path.join(os.getcwd(), "data", "uber.db")

    #for name, frame in transform.init_transform():
    #    load.create_table_from_dataframe(frame, db_path, name)


if __name__ == '__main__':
    main()
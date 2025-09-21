from source.extract import Extract
from source.transform import Transform

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

    for name, frame in transform.init_transform():
        print(name, frame.columns)

    

if __name__ == '__main__':
    main()
from pandas import DataFrame, concat, NA

import numpy as np

class Transform:

    def __init__(
        self, 
        frame: DataFrame,
        reason_columns,
        payment_columns,
        status_columns,
        car_model_columns,
        incomplete_columns
    ):
        self.frame = frame
        self.copy_frame = frame.copy(deep=True)
        self.reason_columns = reason_columns
        self.payment_columns = payment_columns
        self.status_columns = status_columns
        self.car_model_columns = car_model_columns
        self.incomplete_columns = incomplete_columns

    def init_transform(self) -> list[tuple[str, DataFrame]]:
        print('Iniciando transformação dos dataframes...')

        reason = self.__create_new_frame__(self.frame, self.reason_columns, 'reason', replace_original_frame=False)

        payment_method = self.__create_new_frame__(self.frame, self.payment_columns, 'method', replace_name='payment_method')
        print(payment_method.head(20))
        status = self.__create_new_frame__(self.frame, self.status_columns, 'status', replace_name='booking_status')
        car_model = self.__create_new_frame__(self.frame, self.car_model_columns, 'model', replace_name='car_model')
        #incomplete = self.__create_new_frame__(self.frame, self.incomplete_columns, 'incomplete')

        cancel = self.__create_cancel_frame__(self.frame, reason)
        print('Transformação dos dataframes finalizada.')

        #self.frame.to_csv('tmp/testing.csv')

        return [
            ('reason', reason),
            ('payment_method', payment_method),
            ('status', status),
            ('car_model', car_model),
            #('incomplete', incomplete),
            ('cancel', cancel)
        ]

    def __create_new_frame__(
            self, 
            frame: DataFrame, 
            columns, 
            result,
            replace_original_frame=True,
            replace_name='',
            increment_id=True, 
            unique=True
    ) -> DataFrame:
        sep_columns = frame.filter(items=columns)
        unique_column = len(columns) == 1

        if unique:
            if unique_column:
                sep_columns = self.__get_unique__(sep_columns, columns[0])
            else:
                new_frame = DataFrame()

                for column in columns:
                    new_frame = concat(
                        [new_frame, DataFrame(self.__get_unique__(sep_columns, column))], 
                        axis=0
                    )

                sep_columns = new_frame

        if increment_id:
            if unique_column:
                sep_columns = DataFrame(sep_columns)

            sep_columns = sep_columns.reset_index(drop=True)
            sep_columns['index'] = sep_columns.index

        sep_columns = sep_columns.rename(columns={0: result})

        if replace_original_frame:
            mapping = dict(zip(sep_columns[result], sep_columns['index']))

            self.frame[replace_name] = self.frame[columns[0]].map(mapping)
            self.frame = self.frame.drop(columns, axis=1)

        return sep_columns

    def __get_unique__(self, frame, column):
        return frame[column].dropna().unique()
    
    def __create_cancel_frame__(self, frame: DataFrame, reason: DataFrame) -> DataFrame:
        columns = ['Booking ID',
            'Cancelled Rides by Customer', 'Reason for cancelling by Customer',
            'Cancelled Rides by Driver', 'Driver Cancellation Reason'
        ]

        cancel_df = (frame[columns]
            .dropna(how='all')
            .fillna(-1)
        )

        cancel_df['By_Driver'] = np.where(cancel_df['Cancelled Rides by Driver'] == 1, 1, 0)

        cancel_df['Reason'] = np.where(
            cancel_df['By_Driver'] == 1,
            cancel_df['Driver Cancellation Reason'],
            cancel_df['Reason for cancelling by Customer']
        )

        reason_map = dict(zip(reason['reason'], reason['index']))
        cancel_df['Reason'] = cancel_df['Reason'].map(reason_map)

        cancel_df = cancel_df.reset_index(drop=True)
        cancel_df['index'] = cancel_df.index
        cancel_df = cancel_df.filter(items=['Booking ID', 'index', 'Reason', 'By_Driver'])

        # Tratando frame original
        self.frame = self.frame.drop(columns[1:], axis=1)
        booking_to_index = dict(zip(cancel_df['Booking ID'], cancel_df['index']))
        self.frame['cancel'] = np.where(
            self.frame['Booking ID'].isin(cancel_df['Booking ID']),
            self.frame['Booking ID'].map(booking_to_index),
            NA
        )

        return cancel_df

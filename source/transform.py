from pandas import DataFrame, concat

class Transform:

    def __init__(
        self, 
        frame: DataFrame,
        reason_columns,
        payment_columns,
        status_columns,
        car_model_columns
    ):
        self.frame = frame
        self.reason_columns = reason_columns
        self.payment_columns = payment_columns
        self.status_columns = status_columns
        self.car_model_columns = car_model_columns

    def init_transform(self) -> list[tuple[str, DataFrame]]:
        reason = self.__create_new_frame__(self.frame, self.reason_columns)
        payment_method = self.__create_new_frame__(self.frame, self.payment_columns)
        status = self.__create_new_frame__(self.frame, self.status_columns)
        car_model = self.__create_new_frame__(self.frame, self.car_model_columns)

        return [
            ('reason', reason),
            ('payment_method', payment_method),
            ('status', status),
            ('car_model', car_model)
        ]

    def __create_new_frame__(self, frame: DataFrame, columns, increment_id=True, unique=True) -> DataFrame:
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

            sep_columns = sep_columns.reset_index()

        return sep_columns

    def __get_unique__(self, frame, column):
        return frame[column].dropna().unique()

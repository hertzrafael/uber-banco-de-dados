from pandas import DataFrame

class Transform:

    def __init__(self, frame: DataFrame):
        self.frame = frame

    def init_transform(self) -> list[DataFrame]:
        pass

    def __create_new_frame__(self, frame, name, columns) -> DataFrame:
        pass

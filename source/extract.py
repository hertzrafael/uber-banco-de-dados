from pandas import read_csv, DataFrame

class Extract:

    def __init__(self):
        pass

    def run(self, file_path='files/ncr_ride_bookings.csv', sep=',') -> DataFrame:
        print(f'Iniciando carregamento de dados do arquivo no caminho {file_path}')
        frame = read_csv(file_path, sep=sep)

        print('Dataframe carregado com sucesso:')
        print(frame.head(5))

        return frame

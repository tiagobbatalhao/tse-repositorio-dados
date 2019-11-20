import pandas
import logging
import os
import glob
import re

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger()

class File_Zonas():

    folder = os.path.expanduser(
        os.path.join(
            '~',
            'localdatalake',
            'tse_raw',
            'geografico',
            'zonas_tse'
        )
    )
    filename = 'lista_zonas_eleitorais_*.csv'
    source = 'http://www.tse.jus.br/eleitor/cartorios-e-zonas-eleitorais/pesquisa-a-zonas-eleitorais'

    @classmethod
    def find_files(cls):
        fls = sorted(glob.glob(os.path.join(cls.folder, cls.filename)))
        return fls

    @classmethod
    def read_file(cls, index=0):
        filename = cls.find_files()[index]
        df = pandas.read_csv(filename, sep=',', encoding='latin1', header=0)
        df.columns = [
            'Zona',
            'id',
            'Endereço',
            'CEP',
            'Bairro',
            'Município_nome',
            'UF',
        ]
        df['CEP'] = df['CEP'].apply(lambda x: str(x).zfill(8))
        df = df[[
            'id',
            'UF',
            'Zona',
            'CEP',
            'Endereço',
            'Bairro',
            'Município_nome',
        ]]
        return df.sort_values(by=['UF', 'Zona'])



def main():

    obj = File_Zonas()
    files = obj.find_files()
    df_out = pandas.concat([
        obj.read_file(x)
        for x in range(len(files))
    ])

    folder = os.path.expanduser(
        os.path.join(
            '~',
            'localdatalake',
            'tse_refined',
            'geografico',
        )
    )
    filename = 'ZonasEleitorais_BR.csv'

    df_out.to_csv(
        os.path.join(folder, filename),
        sep=',',
        header=True,
        index=False,
    )
    return df_out

if __name__ == '__main__':
    main()
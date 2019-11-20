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
        )
    )
    filename = 'zonas-eleitorais.csv'
    source = 'https://github.com/mapaslivres/zonas-eleitorais'

    @classmethod
    def find_files(cls):
        fls = sorted(glob.glob(os.path.join(cls.folder, cls.filename)))
        return fls

    @classmethod
    def read_file(cls):
        filename = cls.find_files()[0]
        df = pandas.read_csv(filename, sep=',', encoding='utf-8', header=0)
        df = df.rename(columns={
            'endereco_tse': 'Endereço',
            'cep': 'CEP',
            'bairro': 'Bairro',
            'nome_municipio': 'Município_nome',
            'uf': 'UF',
            'municipio_id': 'Município_id',
        })
        df['Zona'] = df['id'].apply(lambda x: int(x.split('-')[1]))
        df['CEP'] = df['CEP'].apply(lambda x: str(x).zfill(8))
        df = df[[
            'id',
            'UF',
            'Zona',
            'CEP',
            'Endereço',
            'Bairro',
            'Município_id',
            'Município_nome',
        ]]
        return df.sort_values(by=['UF', 'Zona'])


def main():

    obj = File_Zonas()
    files = obj.find_files()
    df_out = obj.read_file()

    folder = os.path.expanduser(
        os.path.join(
            '~',
            'localdatalake',
            'tse_refined',
            'geografico',
        )
    )
    filename = 'ZonasEleitorais.csv'

    df_out.to_csv(
        os.path.join(folder, filename),
        sep=',',
        header=True,
        index=False,
    )
    return df_out

if __name__ == '__main__':
    main()
import pandas
import logging
import os
import glob
import re

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger()

class FilesRaw:

    @classmethod
    def find_files(cls):
        fls = sorted(glob.glob(os.path.join(cls.folder, cls.filename)))
        return fls

class File_SecoesSP(FilesRaw):

    folder = os.path.expanduser(
        os.path.join(
            '~',
            'localdatalake',
            'tse_raw',
            'geografico',
        )
    )
    source = 'http://www.tre-sp.jus.br/eleitor/titulo-e-local-de-votacao/consulta-por-zona-eleitoral-e-bairro'

    @classmethod
    def parse_list_secoes(cls, list_as_string):
        special = 'ª'
        output = []
        try:
            for s in list_as_string.split(';'):
                output += cls.parse_string_secao(s)
        except AttributeError:
            return []
        return output

    @classmethod
    def parse_string_secao(cls, s):
        ss = s.strip().strip('.')
        
        exp = re.compile('Da ([0-9]{1,})ª à ([0-9]{1,})ª')
        match = exp.match(ss)
        if match:
            numbers = [int(x) for x in match.groups()]
            return list(range(numbers[0], numbers[1]+1))

        exp = re.compile('([0-9]{1,})ª')
        match = exp.match(ss)
        if match:
            numbers = [int(x) for x in match.groups()]
            return [numbers[0]]

        if len(ss):
            logger.warning('Could not parse: {}'.format(s))
        return []

    @classmethod
    def parse_string_nome(cls, s):
        ss = s.strip().strip('.')
        exp = re.compile('([0-9]{1,})a')
        match = exp.match(ss)
        if match:
            numbers = [int(x) for x in match.groups()]
            return numbers[0]

    @classmethod
    def normalize(cls, df_in):
        """
        Coloca cada seção em uma linha diferente
        """
        df_v2 = df_in.copy()
        df_v2['Seções'] = df_v2['Seções'].apply(cls.parse_list_secoes)
        df_v2['SeçõesEspeciais'] = df_v2['SeçõesEspeciais'].apply(cls.parse_list_secoes)
        df_v2['SeçõesNao'] = df_v2.apply(
            lambda row: [x for x in row['Seções'] if x not in row['SeçõesEspeciais']],
            axis=1,
        )
        df_v2['Zona'] = df_v2['Nome'].apply(cls.parse_string_nome)
        dataframes = []
        columns = [x for x in df_v2.columns if not x.startswith('Seções')]
        for l, row in df_v2.iterrows():
            df = pandas.DataFrame(
                sorted([(x, False) for x in row['SeçõesNao']]+[(x, True) for x in row['SeçõesEspeciais']]),
                columns=['Seção', 'Especial'],
            )
            for col in columns:
                df[col] = row[col]
            dataframes.append(df)
        return pandas.concat(dataframes)


class File_SecoesSPestado(File_SecoesSP):

    filename = 'arquivo(1).csv'

    @classmethod
    def read_file(cls):
        filename = cls.find_files()[0]
        df = pandas.read_csv(filename, sep=',', encoding='latin1', header=None)
        df.columns = [
            'Nome',
            'Local',
            'Endereço',
            'Município',
            'Seções',
            'SeçõesEspeciais',
        ]
        return df

class File_SecoesSPcapital(File_SecoesSP):

    filename = 'arquivo.csv'

    @classmethod
    def read_file(cls):
        filename = cls.find_files()[0]
        df = pandas.read_csv(filename, sep=',', encoding='latin1', header=None)
        df.columns = [
            'Bairro',
            'Nome',
            'Local',
            'Endereço',
            'Seções',
            'SeçõesEspeciais',
        ]
        return df


def main():

    dfA_in = File_SecoesSPestado().read_file()
    dfA_out = File_SecoesSP.normalize(dfA_in)
    dfB_in = File_SecoesSPcapital().read_file()
    dfB_out = File_SecoesSP.normalize(dfB_in)

    index = ['Zona', 'Seção']
    columns = ['Nome', 'Local', 'Endereço', 'Município', 'Bairro', 'Especial']
    df_out = (
        dfA_out
        .merge(
            dfB_out[index+['Bairro']],
            how='outer',
            on=index,
        )
        .sort_values(by=index)
        [index+columns]
    )

    folder = os.path.expanduser(
        os.path.join(
            '~',
            'localdatalake',
            'tse_refined',
            'geografico',
        )
    )
    filename = 'SecoesEleitorais_SP.csv'

    df_out.to_csv(
        os.path.join(folder, filename),
        sep=',',
        header=True,
        index=False,
    )
    return df_out

if __name__ == '__main__':
    main()

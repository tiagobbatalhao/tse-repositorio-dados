import requests
import zipfile
import io
import pandas
import logging
import os
import re
import argparse


from slugify import slugify
from datetime import datetime, timedelta

ESTADOS_TODOS = [
    'AC', 'RR', 'TO', 'PA', 'AM', 'AP', 'RO',
    'BA', 'AL', 'SE', 'PE', 'RN', 'PB', 'CE', 'PI', 'MA',
    'GO', 'DF', 'MS', 'MT',
    'RS', 'SC', 'PR',
    'MG', 'RJ', 'ES', 
    'SP', 
]

class TSE_download():
    
    url = 'http://agencia.tse.jus.br/estatistica/sead/odsele/'
    folder_save = os.path.expanduser('~/localdatalake/tse_raw/originals')
    
    @classmethod
    def download(cls, path='', **kwargs):

        ## First, check the already downloaded files
        save_name = os.path.join(cls.folder_save, 'zipped', path)
        if os.path.exists(save_name):
            print('Reading local file: {}'.format(save_name))
            with open(save_name, 'rb') as flread:
                content = io.BytesIO(flread.read())
        else:
            this_url = os.path.join(cls.url, path)
            req = requests.get(this_url)
            try:
                content = io.BytesIO(req.content)
                logging.info(f'Done')
            except zipfile.BadZipFile:
                logging.error(f'Not a valid file')
                return None

        if kwargs.get('save', False):
            save_name = os.path.join(cls.folder_save, 'zipped', path)
            folder = os.path.dirname(save_name)
            if not os.path.isdir(folder):
                os.makedirs(folder)
            with open(save_name, 'wb') as flsave:
                flsave.write(content.read())
        elif kwargs.get('save_unzipped', False):
            zipped = zipfile.ZipFile(content)
            for name in zipped.namelist():
                save_name = os.path.join(cls.folder_save, 'unzipped', path.split('.')[0], name)
                folder = os.path.dirname(save_name)
                if not os.path.isdir(folder):
                    os.makedirs(folder)
                with open(save_name, 'wb') as flsave:
                    with zipped.open(name) as flread:
                        flsave.write(flread.read())
        elif kwargs.get('return_unzipped'):
            files = {}
            zipped = zipfile.ZipFile(content)
            for name in zipped.namelist():
                with zipped.open(name) as flread:
                    files[name] = flread.read()
            return files
        else:
            return zipfile.ZipFile(content)

    @classmethod
    def read_files(cls, file_dict, header=0):
        dataframes = {}
        for name, fl in file_dict.items():
            if name.endswith('.csv') or name.endswith('txt'):
                dataframes['.'.join(name.split('.')[:-1])] = pandas.read_csv(
                    io.BytesIO(fl),
                    sep=';',
                    header=header,
                    encoding='latin1',
                )
        return dataframes

class TSE_download_demografia_zona(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'perfil_eleitorado/perfil_eleitorado_{ano}.zip'
        return super().download(path, **kwargs)

class TSE_download_demografia_secao(TSE_download):
    @classmethod
    def download(cls, ano, estado, **kwargs):
        path = f'perfil_eleitor_secao/perfil_eleitor_secao_{ano}_{estado}.zip'
        return super().download(path, **kwargs)
    
class TSE_download_candidatos(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'consulta_cand/consulta_cand_{ano}.zip'
        return super().download(path, **kwargs)
    
class TSE_download_votacao_candidato_zona(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'votacao_candidato_munzona/votacao_candidato_munzona_{ano}.zip'
        return super().download(path, **kwargs)
    
class TSE_download_votacao_partido_zona(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'votacao_partido_munzona/votacao_partido_munzona_{ano}.zip'
        return super().download(path, **kwargs)
    
class TSE_download_votacao_secao(TSE_download):
    @classmethod
    def download(cls, ano, estado, **kwargs):
        path = f'votacao_secao/votacao_secao_{ano}_{estado}.zip'
        return super().download(path, **kwargs)

class TSE_download_votacao_detalhezona(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}.zip'
        return super().download(path, **kwargs)

class TSE_download_votacao_detalhesecao(TSE_download):
    @classmethod
    def download(cls, ano, **kwargs):
        path = f'detalhe_votacao_secao/detalhe_votacao_secao_{ano}.zip'
        return super().download(path, **kwargs)
    
class TSE_parse:

    @classmethod
    def get_dicionario(cls):
        return cls.tabelas_dicionario
    
    @classmethod
    def get_dicionario_pandas(cls):
        dfs = {}
        for key, value in cls.tabelas_dicionario.items():
            df = pandas.DataFrame(value.items())
            df.columns = [key, key+'_desc']
            dfs[key] = df
        return dfs

    @staticmethod
    def parse_data(text):
        try:
            return datetime.strptime(text, '%d/%m/%Y').date()
        except (TypeError, ValueError):
            return None

    @staticmethod
    def parse_integer(text):
        try:
            return int(text)
        except (TypeError, ValueError):
            return -1

    @staticmethod
    def parse_float(text):
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

class TSE_parse_demografia(TSE_parse):

    tabelas_dicionario = {
        'Gênero': {
            2: 'Masculino',
            4: 'Feminino',
        },
        'Escolaridade': {
            1: 'Analfabeto',
            2: 'Alfabetizado',
            3: 'Fundamental incompleto',
            4: 'Fundamental completo',
            5: 'Médio incompleto',
            6: 'Médio completo',
            7: 'Superior incompleto',
            8: 'Superior completo',
        },
        'EstadoCivil': {
            1: 'Solteiro',
            3: 'Casado',
            5: 'Viúvo',
            7: 'Separado',
            9: 'Divorciado',
        },
        'FaixaEtária': {
            '100-': '100-',
            '16-16': '16-16',
            '17-17': '17-17',
            '18-20': '18-20',
            '18-18': '18-18',
            '19-19': '19-19',
            '20-20': '20-20',
            '21-24': '21-24',
            '25-29': '25-29',
            '25-34': '25-34',
            '30-34': '30-34',
            '35-39': '35-39',
            '35-44': '35-44',
            '40-44': '40-44',
            '45-49': '45-49',
            '45-59': '45-59',
            '50-54': '50-54',
            '55-59': '55-59',
            '60-64': '60-64',
            '60-69': '60-69',
            '65-69': '65-69',
            '70-74': '70-74',
            '70-79': '70-79',
            '75-79': '75-79',
            '80-84': '80-84',
            '85-89': '85-89',
            '90-94': '90-94',
            '95-99': '95-99',
            '80-': '80-',   
        }
    }
    
    tabelas_depara = {
        'Gênero' : {
            'feminino': 4,
            'masculino': 2,
        },
        'Escolaridade' : {
            'analfabeto': 1,
            'ensino-fundamental-completo': 4,
            'ensino-fundamental-incompleto': 3, 
            'ensino-medio-completo': 6,
            'ensino-medio-incompleto': 5,
            'le-e-escreve': 2,
            'primeiro-grau-completo': 4,
            'primeiro-grau-incompleto': 3, 
            'segundo-grau-completo': 6,
            'segundo-grau-incompleto': 5,
            'superior-completo': 8,
            'superior-incompleto': 7,
        },
        'EstadoCivil' : {
            'casado': 3,
            'divorciado': 9,
            'separado-judicialmente': 7,
            'solteiro': 1,
            'viuvo': 5,
        },
        'FaixaEtária' : {
            '100-anos-ou-mais': '100-',
            '16-anos': '16-16',
            '17-anos': '17-17',
            '18-a-20-anos': '18-20',
            '18-anos': '18-18',
            '19-anos': '19-19',
            '20-anos': '20-20',
            '21-a-24-anos': '21-24',
            '25-a-29-anos': '25-29',
            '25-a-34-anos': '25-34',
            '30-a-34-anos': '30-34',
            '35-a-39-anos': '35-39',
            '35-a-44-anos': '35-44',
            '40-a-44-anos': '40-44',
            '45-a-49-anos': '45-49',
            '45-a-59-anos': '45-59',
            '50-a-54-anos': '50-54',
            '55-a-59-anos': '55-59',
            '60-a-64-anos': '60-64',
            '60-a-69-anos': '60-69',
            '65-a-69-anos': '65-69',
            '70-a-74-anos': '70-74',
            '70-a-79-anos': '70-79',
            '75-a-79-anos': '75-79',
            '80-a-84-anos': '80-84',
            '85-a-89-anos': '85-89',
            '90-a-94-anos': '90-94',
            '95-a-99-anos': '95-99',
            'superior-a-79-anos': '80-'
        },
    }
    
    @classmethod
    def list_municipios(cls, file_object, ano, nivel=None):
        if (str(ano) in ['2018', 'ATUAL']):
            columns = [
                ('SG_UF', 'UF'),
                ('CD_MUNICIPIO', 'Município'),
                ('NM_MUNICIPIO', 'Nome'),
            ]
            header = 0
        elif (str(ano) not in ['2018', 'ATUAL']) and (nivel == 'zona'):
            columns = [
                (1, 'UF'),
                (3, 'Município'),
                (2, 'Nome'),
            ]
            header = 0
        elif (str(ano) not in ['2018', 'ATUAL']) and (nivel == 'secao'):
            columns = [
                (3, 'UF'),
                (4, 'Município'),
                (5, 'Nome'),
            ]
            header = 0
        
        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
            )
            .rename(columns={x[0]: x[1] for x in columns})
            [[x[1] for x in columns]]
            .dropna()
            .drop_duplicates()
            
        )
        df['Município'] = df['Município'].apply(int)
        df = df.sort_values(by=['Município']).reset_index(drop=True)
        df['UF'] = df['UF'].apply(lambda x: str(x)[:2])
        df['slugified'] = df.apply(lambda x: "{}|{}".format(slugify(x['Nome']), slugify(x['UF'])), axis=1)
        return df

    @classmethod
    def parse(cls, file_object, ano, nivel, **kwargs):
        
        if (str(ano) in ['2018', 'ATUAL']) and (nivel == 'zona'):
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('SG_UF', 'UF'),
                ('CD_MUNICIPIO', 'Município'),
                ('NR_ZONA', 'Zona'),
                ('DS_GENERO', 'Gênero'),
                ('DS_ESTADO_CIVIL', 'EstadoCivil'),
                ('DS_GRAU_ESCOLARIDADE', 'Escolaridade'),
                ('DS_FAIXA_ETARIA', 'FaixaEtária'),
                ('QT_ELEITORES_PERFIL', 'Quantidade'),
                ('QT_ELEITORES_DEFICIENCIA', 'QuantidadeDeficiência'),
                ('QT_ELEITORES_INC_NM_SOCIAL', 'QuantidadeNomeSocial'),
            ]
            columns_extra = []
            header = 0
        elif (str(ano) not in ['2018', 'ATUAL']) and (nivel == 'zona'):
            columns = [
                (0, 'Ano'),
                (1, 'UF'),
                (3, 'Município'),
                (4, 'Zona'),
                (5, 'Gênero'),
                (7, 'Escolaridade'),
                (6, 'FaixaEtária'),
                (8, 'Quantidade'),
            ]
            columns_extra = [
                'EstadoCivil',
                'QuantidadeDeficiência',
                'QuantidadeNomeSocial',
            ]
            header = None
        elif (str(ano) in ['2018', 'ATUAL']) and (nivel == 'secao'):
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('SG_UF', 'UF'),
                ('CD_MUNICIPIO', 'Município'),
                ('NR_ZONA', 'Zona'),
                ('NR_SECAO', 'Seção'),
                ('DS_GENERO', 'Gênero'),
                ('DS_ESTADO_CIVIL', 'EstadoCivil'),
                ('DS_GRAU_ESCOLARIDADE', 'Escolaridade'),
                ('DS_FAIXA_ETARIA', 'FaixaEtária'),
                ('QT_ELEITORES_PERFIL', 'Quantidade'),
                ('QT_ELEITORES_DEFICIENCIA', 'QuantidadeDeficiência'),
                ('QT_ELEITORES_INC_NM_SOCIAL', 'QuantidadeNomeSocial'),
            ]
            columns_extra = []
            header = 0
        elif (str(ano) not in ['2018', 'ATUAL']) and (nivel == 'secao'):
            columns = [
                (2, 'Ano'),
                (3, 'UF'),
                (4, 'Município'),
                (6, 'Zona'),
                (7, 'Seção'),
                (9, 'EstadoCivil'),
                (15, 'Gênero'),
                (13, 'Escolaridade'),
                (11, 'FaixaEtária'),
                (16, 'Quantidade'),
            ]
            columns_extra = [
                'QuantidadeDeficiência',
                'QuantidadeNomeSocial',
            ]
            header = None
        
        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
            )
            .rename(columns={x[0]: x[1] for x in columns})
            [[x[1] for x in columns]]
            .reset_index(drop=True)
        )
        for col in columns_extra:
            df[col] = None

        for col, depara in cls.tabelas_depara.items():
            df[col] = df[col].apply(lambda x: depara.get(slugify(str(x).strip()), 0))

        df['Ano'] = df['Ano'].apply(lambda x: int(str(x)[:4]))
        df['UF'] = df['UF'].apply(lambda x: str(x)[:2])

        if kwargs.get('use_category'):
            categorical = ['UF', 'FaixaEtária']
            for col in categorical:
                if col in df.columns:
                    df[col] = df[col].astype('category')        
        return df
    

    

class TSE_parse_candidatos(TSE_parse):

    tabelas_dicionario = {
        'Gênero': {
            2: 'Masculino',
            4: 'Feminino',
        },
        'Escolaridade': {
            1: 'Analfabeto',
            2: 'Alfabetizado',
            3: 'Fundamental incompleto',
            4: 'Fundamental completo',
            5: 'Médio incompleto',
            6: 'Médio completo',
            7: 'Superior incompleto',
            8: 'Superior completo',
        },
        'EstadoCivil': {
            1: 'Solteiro',
            3: 'Casado',
            5: 'Viúvo',
            7: 'Separado',
            9: 'Divorciado',
        },
        'Cor': {
            1: 'Branca',
            2: 'Preta',
            3: 'Parda',
            4: 'Amarela',
            5: 'Indígena',
        },
        'Cargo' : {
            1: 'Presidente',
            2: 'Vice-Presidente',
            3: 'Governador',
            4: 'Vice-Governador',
            5: 'Senador',
            6: 'Deputado Federal',
            7: 'Deputado Estadual',
            8: 'Deputado Distrital',
            9: 'Suplente 1 de Senador',
            10: 'Suplente 2 de Senador',
            11: 'Prefeito',
            12: 'Vice-Prefeito',
            13: 'Vereador',
        }
    }
    
    tabelas_depara = {
        'Gênero' : {
            'feminino': 4,
            'masculino': 2,
        },
        'Escolaridade' : {
            'analfabeto': 1,
            'ensino-fundamental-completo': 4,
            'ensino-fundamental-incompleto': 3, 
            'ensino-medio-completo': 6,
            'ensino-medio-incompleto': 5,
            'le-e-escreve': 2,
            'primeiro-grau-completo': 4,
            'primeiro-grau-incompleto': 3, 
            'segundo-grau-completo': 6,
            'segundo-grau-incompleto': 5,
            'superior-completo': 8,
            'superior-incompleto': 7,
        },
        'EstadoCivil' : {
            'casado': 3,
            'divorciado': 9,
            'separado-judicialmente': 7,
            'solteiro': 1,
            'viuvo': 5,
            'casado-a': 3,
            'divorciado-a': 9,
            'separado-a-judicialmente': 7,
            'solteiro-a': 1,
            'viuvo-a': 5,
        },
        'Cor' : {
            'branca': 1,
            'preta': 2,
            'parda': 3,
            'amarela': 4,
            'indigena': 5,
        },
    }
    
    @classmethod
    def parse(cls, file_object, ano, **kwargs):
        
        if int(ano) >= 2014:
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('NR_TURNO', 'Turno'),
                ('DS_ELEICAO', 'Eleição_nome'),
                ('DT_ELEICAO', 'Eleição_data'),
                ('SG_UF', 'UF'),
                ('SG_UE', 'UE'),
                ('CD_CARGO', 'Cargo'),
                ('SQ_CANDIDATO', 'id'),
                ('NR_CPF_CANDIDATO', 'Documento_CPF'),
                ('NM_CANDIDATO', 'Nome_completo'),
                ('NM_URNA_CANDIDATO', 'Urna_nome'),
                ('NM_SOCIAL_CANDIDATO', 'Nome_social'),
                ('NR_CANDIDATO', 'Urna_número'),
                ('DS_SITUACAO_CANDIDATURA', 'Situação'),
                ('DS_DETALHE_SITUACAO_CAND', 'Situação_detalhe'),
                ('TP_AGREMIACAO', 'Agremiação'),
                ('NR_PARTIDO', 'Partido_número'),
                ('SG_PARTIDO', 'Partido_sigla'),
                ('NM_PARTIDO', 'Partido_nome'),
                ('SQ_COLIGACAO', 'Coligação_código'),
                ('DS_COMPOSICAO_COLIGACAO', 'Coligação_composição'),
                ('DS_NACIONALIDADE', 'Nacionalidade'),
                ('NR_IDADE_DATA_POSSE', 'Idade'),
                ('SG_UF_NASCIMENTO', 'Nascimento_UF'),
                ('CD_MUNICIPIO_NASCIMENTO', 'Nascimento_município'),
                ('DT_NASCIMENTO', 'Nascimento_data'),
                ('DS_OCUPACAO', 'Ocupação'),
                ('NR_TITULO_ELEITORAL_CANDIDATO', 'Documento_título'),
                ('DS_GENERO', 'Gênero'),
                ('DS_GRAU_INSTRUCAO', 'Escolaridade'),
                ('DS_ESTADO_CIVIL', 'EstadoCivil'),
                ('DS_COR_RACA', 'Cor'),
                ('NR_DESPESA_MAX_CAMPANHA', 'Despesa'),
                ('DS_SIT_TOT_TURNO', 'Totalização'),
                ('ST_REELEICAO', 'Reeleição'),
                ('ST_DECLARAR_BENS', 'Declaração'),
            ]
            columns_extra = []
            header = 0
        elif ano <= 2012:
            columns = [
                (2, 'Ano'),
                (3, 'Turno'),
                (4, 'Eleição_nome'),
                (5, 'UF'),
                (6, 'UE'),
                (8, 'Cargo'),
                (11, 'id'),
                (13, 'Documento_CPF'),
                (10, 'Nome_completo'),
                (14, 'Urna_nome'),
                (12, 'Urna_número'),
                (16, 'Situação'),
                (17, 'Partido_número'),
                (18, 'Partido_sigla'),
                (19, 'Partido_nome'),
                (20, 'Coligação_código'),
                (22, 'Coligação_composição'),
                (36, 'Nacionalidade'),
                (28, 'Idade'),
                (37, 'Nascimento_UF'),
                (38, 'Nascimento_município'),
                (26, 'Nascimento_data'),
                (25, 'Ocupação'),
                (27, 'Documento_título'),
                (30, 'Gênero'),
                (32, 'Escolaridade'),
                (34, 'EstadoCivil'),
                (40, 'Despesa'),
                (42, 'Totalização'),
            ]
            columns_extra = [
                'Eleição_data',
                'Nome_social',
                'Situação_detalhe',
                'Agremiação',
                'Cor',
                'Reeleição',
                'Declaração',
            ]
            header = None

        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
                dtype='str',
            )
            .rename(columns={x[0]: x[1] for x in columns})
            [[x[1] for x in columns]]
            .reset_index(drop=True)
        )
        for col in columns_extra:
            df[col] = None
            
        for col, depara in cls.tabelas_depara.items():
            df[col] = df[col].apply(lambda x: depara.get(slugify(str(x).strip()), 0)).astype(int)
            
        for col in df.select_dtypes(include=['object']).columns:
            flter = df[col].apply(lambda x: x in ['#NULO#','#NE#'])
            if sum(flter)>0:
                df.loc[flter, col] = None
        
        for col in ['Nascimento_data', 'Eleição_data']:
            df[col] = df[col].apply(cls.parse_data)
        
        if kwargs.get('use_category'):
            categorical = [
                'Eleição_nome',
                'UF',
                'UE',
                'Partido_sigla',
                'Partido_nome',
                'Situação',
                'Situação_detalhe',
                'Agremiação',
                'Nacionalidade',
                'Nascimento_UF',
                'Totalização',
                'Reeleição',
                'Declaração',
            ]
            for col in categorical:
                if col in df.columns:
                    df[col] = df[col].astype('category')
    
        numeric_integer = ['Ano', 'Turno', 'Cargo', 'Urna_número', 'Partido_número', 'Idade', 'Nascimento_município']
        numeric_float = ['Despesa']
        for col in numeric_integer:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_integer(x)).astype(int)
        for col in numeric_float:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_float(x)).astype(float)
                df.loc[df[col]<0, col] = None
    
        return df

        

class TSE_parse_votacao_candidato(TSE_parse):

    @classmethod
    def parse(cls, file_object, ano, nivel, **kwargs):
        
        if int(ano) >= 2018:
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('NR_TURNO', 'Turno'),
                ('DS_ELEICAO', 'Eleição_nome'),
                ('SG_UF', 'UF'),
                ('SG_UE', 'UE'),
                ('CD_CARGO', 'Cargo'),
                ('CD_MUNICIPIO', 'Município'),
                ('NR_ZONA', 'Zona'),
                ('NR_VOTAVEL', 'Urna_número'),
                ('QT_VOTOS', 'Votos'),
            ]
            if nivel == 'secao':
                columns += [('NR_SECAO', 'Seção'),]
            columns_extra = []
            header = 0
        elif ano <= 2016:
            shift_secao = 1 if nivel == 'secao' else 0
            columns = [
                (2, 'Ano'),
                (3, 'Turno'),
                (4, 'Eleição_nome'),
                (5, 'UF'),
                (6, 'UE'),
                (10+shift_secao, 'Cargo'),
                (7, 'Município'),
                (9, 'Zona'),
                (12+shift_secao, 'Urna_número'),
                (13+shift_secao, 'Votos'),
            ]
            if nivel == 'secao':
                columns += [(10, 'Seção'),]
            columns_extra = []
            header = None

        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
                dtype='str',
            )
            [[x[0] for x in columns]]
            .rename(columns={x[0]: x[1] for x in columns})
            .reset_index(drop=True)
        )
        for col in columns_extra:
            df[col] = None
                        
        for col in df.select_dtypes(include=['object']).columns:
            flter = df[col].apply(lambda x: x in ['#NULO#','#NE#'])
            if sum(flter)>0:
                df.loc[flter, col] = None

        if kwargs.get('use_categories'):        
            categorical = ['Eleição_nome', 'UF', 'UE',]
            for col in categorical:
                if col in df.columns:
                    df[col] = df[col].astype('category')
    
        numeric_integer = ['Ano', 'Turno', 'Cargo', 'Município', 'Zona', 'Seção', 'Votos', 'Urna_número']
        numeric_float = []
        for col in numeric_integer:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_integer(x)).astype(int)
        for col in numeric_float:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_float(x)).astype(float)
                df.loc[df[col]<0, col] = None
    
        return df

class TSE_parse_votacao_candidato_zona(TSE_parse):

    @classmethod
    def parse(cls, file_object, ano, **kwargs):
        
        if int(ano) >= 2016:
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('NR_TURNO', 'Turno'),
                ('DS_ELEICAO', 'Eleição_nome'),
                ('SG_UF', 'UF'),
                ('SG_UE', 'UE'),
                ('CD_CARGO', 'Cargo'),
                ('CD_MUNICIPIO', 'Município'),
                ('NR_ZONA', 'Zona'),
                ('NR_CANDIDATO', 'Urna_número'),
                ('QT_VOTOS_NOMINAIS', 'Votos'),
            ]
            columns_extra = []
            header = 0
        elif ano <= 2014:
            columns = [
                (2, 'Ano'),
                (3, 'Turno'),
                (4, 'Eleição_nome'),
                (5, 'UF'),
                (6, 'UE'),
                (10, 'Cargo'),
                (7, 'Município'),
                (9, 'Zona'),
                (11, 'Urna_número'),
                (28, 'Votos'),
            ]
            columns_extra = []
            header = None

        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
                dtype='str',
            )
            [[x[0] for x in columns]]
            .rename(columns={x[0]: x[1] for x in columns})
            .reset_index(drop=True)
        )
        for col in columns_extra:
            df[col] = None
                        
        for col in df.select_dtypes(include=['object']).columns:
            flter = df[col].apply(lambda x: x in ['#NULO#','#NE#'])
            if sum(flter)>0:
                df.loc[flter, col] = None

        if kwargs.get('use_categories'):        
            categorical = ['Eleição_nome', 'UF', 'UE',]
            for col in categorical:
                if col in df.columns:
                    df[col] = df[col].astype('category')
    
        numeric_integer = ['Ano', 'Turno', 'Cargo', 'Município', 'Zona', 'Seção', 'Votos', 'Urna_número']
        numeric_float = []
        for col in numeric_integer:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_integer(x)).astype(int)
        for col in numeric_float:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_float(x)).astype(float)
                df.loc[df[col]<0, col] = None
    
        return df

class TSE_parse_votacao_detalhe(TSE_parse):

    @classmethod
    def parse(cls, file_object, ano, nivel, **kwargs):
        
        if int(ano) >= 2018:
            columns = [
                ('ANO_ELEICAO', 'Ano'),
                ('NR_TURNO', 'Turno'),
                ('DS_ELEICAO', 'Eleição_nome'),
                ('SG_UF', 'UF'),
                ('SG_UE', 'UE'),
                ('CD_CARGO', 'Cargo'),
                ('CD_MUNICIPIO', 'Município'),
                ('NR_ZONA', 'Zona'),
                ('QT_APTOS', 'Votos_aptos'),
                ('QT_COMPARECIMENTO', 'Votos_comparecimento'),
                ('QT_ABSTENCOES', 'Votos_abstenções'),
                ('QT_VOTOS_NOMINAIS', 'Votos_nominais'),
                ('QT_VOTOS_BRANCOS', 'Votos_brancos'),
                ('QT_VOTOS_NULOS', 'Votos_nulos'),
                ('QT_VOTOS_LEGENDA', 'Votos_legenda'),
                ('QT_VOTOS_PENDENTES', 'Votos_pendentes'),
            ]
            if nivel == 'secao':
                columns += [('NR_SECAO', 'Seção'),]
            columns_extra = []
            header = 0
        elif ano <= 2016:
            shift_secao = 1 if nivel == 'secao' else 0
            columns = [
                (2, 'Ano'),
                (3, 'Turno'),
                (4, 'Eleição_nome'),
                (5, 'UF'),
                (6, 'UE'),
                (10+shift_secao, 'Cargo'),
                (7, 'Município'),
                (9, 'Zona'),
                (12+shift_secao, 'Votos_aptos'),
                (13+shift_secao, 'Votos_comparecimento'),
                (14+shift_secao, 'Votos_abstenções'),
                (15+shift_secao, 'Votos_nominais'),
                (16+shift_secao, 'Votos_brancos'),
                (17+shift_secao, 'Votos_nulos'),
                (18+shift_secao, 'Votos_legenda'),
                (19+shift_secao, 'Votos_pendentes'),
            ]
            if nivel == 'secao':
                columns += [(10, 'Seção'),]
            columns_extra = []
            header = None

        df = (
            pandas.read_csv(
                io.BytesIO(file_object),
                sep=';',
                header=header,
                encoding='latin1',
                dtype='str',
            )
            [[x[0] for x in columns]]
            .rename(columns={x[0]: x[1] for x in columns})
            .reset_index(drop=True)
        )
        for col in columns_extra:
            df[col] = None
                        
        for col in df.select_dtypes(include=['object']).columns:
            flter = df[col].apply(lambda x: x in ['#NULO#','#NE#'])
            if sum(flter)>0:
                df.loc[flter, col] = None

        if kwargs.get('use_categories'):        
            categorical = ['Eleição_nome', 'UF', 'UE',]
            for col in categorical:
                if col in df.columns:
                    df[col] = df[col].astype('category')
    
        numeric_integer = ['Ano', 'Turno', 'Cargo', 'Município', 'Zona', 'Seção', ] + [x for x in df.columns if x.startswith('Votos')]
        numeric_float = []
        for col in numeric_integer:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_integer(x)).astype(int)
        for col in numeric_float:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.parse_float(x)).astype(float)
                df.loc[df[col]<0, col] = None
    
        return df


class Main:

    anos = list(range(2018, 1998, -2))
    estados = ESTADOS_TODOS

    @classmethod
    def main_loop(cls, anos=None, estados=None, **kwargs):
        for ano in (anos or cls.anos):
            for estado in (estados or cls.estados):
                cls.main(ano=ano, estado=estado, **kwargs)

    @classmethod
    def main(cls, ano=None, estado=None, **kwargs):
        save_name = cls.save_name.format(ano=ano, estado=estado)
        save_full = os.path.join(cls.folder, save_name)
        if kwargs.get('force') or not (os.path.exists(save_full) or os.path.exists(save_full+'.gz')):
            print('[{}] Downloading {}'.format(get_time_now(), save_name))
            if kwargs.get('save_raw'):
                cls.class_downloader.download(ano=ano, estado=estado, save=True)
            download = cls.class_downloader.download(ano=ano, estado=estado)
            print('[{}] Downloaded.'.format(get_time_now()))
            try:
                files = {}
                for name in download.namelist():
                    if (name.endswith('txt') or name.endswith('csv')) and ('brasil' not in name.lower()):
                        with download.open(name) as flread:
                            files[name] = cls.class_parser.parse(
                                flread.read(),
                                ano=ano,
                                **cls.parser_kwargs,
                            )
                print('[{}] Parsed.'.format(get_time_now()))
            # except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
            except (MemoryError, pandas.errors.EmptyDataError):
                print('[{}] PROBLEM: {}'.format(get_time_now(), save_name))
                files = {}
            for name, df in files.items():
                basename = os.path.basename(name)
                groups = cls.regular_expression.match(basename).groups()
                save_name = cls.save_name.format(ano=groups[0], estado=groups[1])
                save_full = os.path.join(cls.folder, save_name)
                if kwargs.get('force') or not (os.path.exists(save_full) or os.path.exists(save_full+'.gz')):
                    df.to_csv(save_full, index=False, header=True, sep=';', float_format='%.0f')
                    print('[{}] Saved {}'.format(get_time_now(), save_name))
        else:
            print('[{}] Found: {}'.format(get_time_now(), save_name))
            pass


class Main_demografia_zona(Main):

    anos = ['ATUAL'] + Main.anos
    estados = [None]
    folder = os.path.expanduser('~/localdatalake/tse_refined/perfil')
    save_name = 'PerfilZona_{ano}.csv'
    class_downloader = TSE_download_demografia_zona
    class_parser = TSE_parse_demografia
    regular_expression = re.compile("perfil_eleitorado_([A-Za-z0-9]{4,}).([a-z]{3})")
    parser_kwargs = dict(nivel='zona')

class Main_demografia_secao(Main):

    anos = ['ATUAL'] + Main.anos
    estados = Main.estados
    folder = os.path.expanduser('~/localdatalake/tse_refined/perfil')
    save_name = 'PerfilSecao_{ano}.csv'
    class_downloader = TSE_download_demografia_secao
    class_parser = TSE_parse_demografia
    regular_expression = re.compile("perfil_eleitor_secao_([0-9A-Za-z]{4,})_([A-Z]{2}).([a-z]{3})")
    parser_kwargs = dict(nivel='secao')

class Main_candidatos(Main):

    anos = Main.anos
    estados = [None]
    folder = os.path.expanduser('~/localdatalake/tse_refined/candidatos')
    save_name = 'Candidatos_{ano}_{estado}.csv'
    class_downloader = TSE_download_candidatos
    class_parser = TSE_parse_candidatos
    regular_expression = re.compile("consulta_cand_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    parser_kwargs = dict()

class Main_votacao_zona(Main):

    anos = Main.anos
    estados = [None]
    folder = os.path.expanduser('~/localdatalake/tse_refined/votos')
    save_name = 'VotoZona_{ano}_{estado}.csv'
    class_downloader = TSE_download_votacao_candidato_zona
    class_parser = TSE_parse_votacao_candidato_zona
    regular_expression = re.compile("votacao_candidato_munzona_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    parser_kwargs = dict()


def download_demografia_zona(anos=None, estados=None, force=False, save=False):
    clsA = TSE_download_demografia_zona()
    clsB = TSE_parse_demografia()
    folder = os.path.expanduser('~/localdatalake/tse_refined/perfil')
    exp = re.compile("perfil_eleitorado_([A-Za-z0-9]{4,}).([a-z]{3})")
    anos = anos or ['ATUAL']+list(range(2018, 2012, -2))
    try:
        for ano in anos:
            save = os.path.join(folder, 'PerfilZona_{}.csv'.format(ano))
            if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                print('[{}] Downloading demografia_zona-{}'.format(get_time_now(), ano))
                download = clsA.download(ano)
                print('[{}] Downloaded.'.format(get_time_now()))
                try:
                    files = {
                        x: clsB.parse(y, ano, nivel='zona')
                        for x, y in download.items()
                        if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                    }
                    print('[{}] Parsed.'.format(get_time_now()))
                except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                    print('[{}] PROBLEMA: {}'.format(get_time_now(), ano))
                    files = {}
                for name, df in files.items():
                    # basename = os.path.basename(name)
                    # ano, extensao = exp.match(basename).groups()
                    df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                    print('[{}] Saved {}'.format(get_time_now(), save))
            else:
                # print('[{}] Found: {}'.format(get_time_now(), save))
                pass
    except KeyboardInterrupt:
        pass


def download_demografia_secao(anos=None, estados=None, force=False):
    clsA = TSE_download_demografia_secao()
    clsB = TSE_parse_demografia()
    folder = os.path.expanduser('~/localdatalake/tse_refined/perfil')
    exp = re.compile("perfil_eleitor_secao_([0-9A-Za-z]{4,})_([A-Z]{2}).([a-z]{3})")
    anos = anos or ['ATUAL']+list(range(2018, 2012, -2))
    estados = estados or ESTADOS_TODOS
    try:
        for ano in anos:
            for estado in estados:
                save = os.path.join(folder, 'PerfilSecao_{}_{}.csv'.format(ano, estado))
                if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                    print('[{}] Downloading demografia_secao-{}-{}'.format(get_time_now(), ano, estado))
                    download = clsA.download(ano, estado)
                    print('[{}] Downloaded.'.format(get_time_now()))
                    try:
                        files = {
                            x: clsB.parse(y, ano, nivel='secao')
                            for x, y in download.items()
                            if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                        }
                        print('[{}] Parsed.'.format(get_time_now()))
                    except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                        print('[{}] PROBLEMA: {}-{}'.format(get_time_now(), ano, estado))
                        files = {}
                    for name, df in files.items():
                        # basename = os.path.basename(name)
                        # ano_s, uf, extensao = exp.match(basename).groups()
                        df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                        print('[{}] Saved {}'.format(get_time_now(), save))
                else:
                    # print('[{}] Found: {}'.format(get_time_now(), save))
                    pass
    except KeyboardInterrupt:
        pass

def download_candidatos(anos=None, force=False):
    clsA = TSE_download_candidatos()
    clsB = TSE_parse_candidatos()
    folder = os.path.expanduser('~/localdatalake/tse_refined/candidatos')
    exp = re.compile("consulta_cand_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    anos = anos or list(range(2018, 2012, -2))
    try:
        for ano in anos:
            print('[{}] Downloading candidatos-{}'.format(get_time_now(), ano))
            download = clsA.download(ano)
            print('[{}] Downloaded.'.format(get_time_now()))
            try:
                files = {
                    x: clsB.parse(y, ano, nivel='secao')
                    for x, y in download.items()
                    if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                }
                print('[{}] Parsed.'.format(get_time_now()))
            except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                print('[{}] PROBLEMA: {}'.format(get_time_now(), ano))
                files = {}
            for name, df in files.items():
                basename = os.path.basename(name)
                ano_s, uf, extensao = exp.match(basename).groups()
                save = os.path.join(folder, 'Candidatos_{}_{}.csv'.format(ano_s, uf))
                if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                    df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                    print('[{}] Saved {}'.format(get_time_now(), save))
    except KeyboardInterrupt:
        pass

def download_votacao_zona(anos=None, force=False):
    clsA = TSE_download_votacao_candidato_zona()
    clsB = TSE_parse_votacao_candidato_zona()
    folder = os.path.expanduser('~/localdatalake/tse_refined/votos')
    exp = re.compile("votacao_candidato_munzona_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    anos = anos or list(range(2018, 2012, -2))
    try:
        for ano in anos:
            print('[{}] Downloading votos_zona-{}'.format(get_time_now(), ano))
            download = clsA.download(ano)
            print('[{}] Downloaded.'.format(get_time_now()))
            try:
                files = {
                    x: clsB.parse(y, ano)
                    for x, y in download.items()
                    if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                }
                print('[{}] Parsed.'.format(get_time_now()))
            except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                print('[{}] PROBLEMA: {}'.format(get_time_now(), ano))
                files = {}
            for name, df in files.items():
                basename = os.path.basename(name)
                ano_s, uf, extensao = exp.match(basename).groups()
                save = os.path.join(folder, 'VotoZona_{}_{}.csv'.format(ano_s, uf))
                if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                    df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                    print('[{}] Saved {}'.format(get_time_now(), save))
    except KeyboardInterrupt:
        pass

def download_votacao_secao(anos=None, estados=None, force=False):
    clsA = TSE_download_votacao_secao()
    clsB = TSE_parse_votacao_candidato()
    folder = os.path.expanduser('~/localdatalake/tse_refined/votos')
    exp = re.compile("votacao_secao_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    anos = anos or list(range(2018, 2012, -2))
    estados = estados or ESTADOS_TODOS + ['BR']
    try:
        for ano in anos:
            for estado in estados:
                save = os.path.join(folder, 'VotoSecao_{}_{}.csv'.format(ano, estado))
                if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                    print('[{}] Downloading votacao_secao-{}-{}'.format(get_time_now(), ano, estado))
                    download = clsA.download(ano, estado)
                    print('[{}] Downloaded.'.format(get_time_now()))
                    try:
                        files = {
                            x: clsB.parse(y, ano, nivel='secao')
                            for x, y in download.items()
                            if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                        }
                        print('[{}] Parsed.'.format(get_time_now()))
                    except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                        print('[{}] PROBLEMA: {}-{}'.format(get_time_now(), ano, estado))
                        files = {}
                    for name, df in files.items():
                        # basename = os.path.basename(name)
                        # ano, uf, extensao = exp.match(basename).groups()
                        df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                        print('[{}] Saved {}'.format(get_time_now(), save))
                else:
                    # print('[{}] Found: {}'.format(get_time_now(), save))
                    pass
    except KeyboardInterrupt:
        pass

def download_votacao_detalhesecao(anos=None, force=False):
    clsA = TSE_download_votacao_detalhesecao()
    clsB = TSE_parse_votacao_detalhe()
    folder = os.path.expanduser('~/localdatalake/tse_refined/votos')
    exp = re.compile("detalhe_votacao_secao_([0-9]{4})_([A-Z]{2}).([a-z]{3})")
    anos = anos or list(range(2018, 2012, -2))
    try:
        for ano in anos:
            print('[{}] Downloading votacao_detalhesecao-{}'.format(get_time_now(), ano))
            download = clsA.download(ano)
            print('[{}] Downloaded.'.format(get_time_now()))
            try:
                files = {
                    x: clsB.parse(y, ano, nivel='secao')
                    for x, y in download.items()
                    if (x.endswith('txt') or x.endswith('csv')) and ('brasil' not in x.lower())
                }
                print('[{}] Parsed.'.format(get_time_now()))
            except (MemoryError, AttributeError, pandas.errors.EmptyDataError):
                print('[{}] PROBLEMA: {}'.format(get_time_now(), ano))
                files = {}
            for name, df in files.items():
                basename = os.path.basename(name)
                ano_s, uf, extensao = exp.match(basename).groups()
                save = os.path.join(folder, 'VotoSecaoDetalhe_{}_{}.csv'.format(ano_s, uf))
                if force or not (os.path.exists(save) or os.path.exists(save+'.gz')):
                    df.to_csv(save, index=False, header=True, sep=';', float_format='%.0f')
                    print('[{}] Saved {}'.format(get_time_now(), save))
    except KeyboardInterrupt:
        pass

def get_time_now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':

    arguments = argparse.ArgumentParser()
    arguments.add_argument('--qual')
    arguments.add_argument('--anos', default=None)
    arguments.add_argument('--force', action='store_true')
    arguments.add_argument('--download', action='store_true')
    parsed = arguments.parse_args()

    def parse_int(x):
        try:
            return int(x)
        except ValueError:
            return str(x)

    if parsed.anos:
        anos = [parse_int(x) for x in str(parsed.anos).split(',')]
    else:
        anos = None
    force = parsed.force
    save = parsed.download

    if parsed.qual in ['candidatos', 'tudo']:
        download_candidatos(anos=anos, force=force)
    if parsed.qual in ['demografia_zona', 'demografia', 'tudo']:
        download_demografia_zona(anos=anos, force=force)
    if parsed.qual in ['demografia_secao', 'demografia', 'tudo']:
        download_demografia_secao(anos=anos, force=force)
    if parsed.qual in ['votos', 'votos_secao', 'tudo']:
        download_votacao_secao(anos=anos, force=force)
    if parsed.qual in ['votos', 'votos_secao', 'tudo']:
        download_votacao_detalhesecao(anos=anos, force=force)
    if parsed.qual in ['votos', 'votos_zona', 'tudo']:
        download_votacao_zona(anos=anos, force=force)

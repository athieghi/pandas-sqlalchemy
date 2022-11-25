import pandas as pd
import datetime
import numpy as np
from infra.configs.connection import DBConnectionHandler
from infra.depara.tickers import get_depara_tickers
from infra.entities.tickers import get_depara_database_tickers



print(f"Iniciando processo....: {datetime.datetime.now()}")

# importa xls

#dataset = pd.read_excel('src/files/PmTesteUnit.xlsx')
dataset = pd.read_excel('src/files/pm.xlsx')


print(f"Lido o arquivo....: {datetime.datetime.now()}")

# renomeia colunas com base no depara
dataset.rename(columns=get_depara_tickers(), inplace=True)

print(f"Renomeando as colunas....: {datetime.datetime.now()}")


print(f"Substituir valor nulo para 0....: {datetime.datetime.now()}")
# Substituir valor nulo para 0 
dataset.fillna(value=0)




print(f"cria ordenação por Conta, Ticekr, DataCompra....: {datetime.datetime.now()}")
dataset = dataset.astype({"Conta":"str","Ticker":"str"})
dataset = dataset.sort_values(by=['Conta', 'Ticker', 'DataCompra'], ascending= True)

print(f"cria indice novo agrupando por ticker....: {datetime.datetime.now()}")
dataset['TickerItem'] = dataset.groupby(['Conta','Ticker']).cumcount()+1

print(f"cria colunas novas conforme EsbocoPm....: {datetime.datetime.now()}")

#cria colunas novas conforme EsbocoPm
dataset['QuantidadeLa'] = 0
dataset['QuantidadeFinal'] = 0
dataset['EstoqueLa'] = 0
dataset['EstoqueMais'] = 0
dataset['EstoqueMenos'] = 0
dataset['DeltaEstoque'] = 0
dataset['EstoqueFinal'] = 0
dataset['PrecoMedio'] = 0

#ordenação
print(f"Nova ordenação Conta, Ticker, DataCompra, TickerItem....Recria indices do dataset: {datetime.datetime.now()}")
dataset = dataset.sort_values(by=['Conta', 'Ticker', 'DataCompra', 'TickerItem'], ascending= True ).reset_index(drop= True)


print(f"realiza as alterações estoque linha anterior....: {datetime.datetime.now()}")

# realiza as alterações estoque linha anterior
for i, row in dataset.iterrows():
    
    
    #print(i, row)
    if row['TickerItem'] == 1:
        dataset.loc[i,'QuantidadeFinal'] = dataset.loc[i,'QuantidadeLa'] + dataset.loc[i,'QuantidadeMovimentacao']
        dataset.loc[i,'EstoqueMais'] = dataset.loc[i,'ValorMovimentacao']  if row['Lancamento'] == 'COMPRA' else 0
        dataset.loc[i,'DeltaEstoque'] = dataset.loc[i,'EstoqueMais'] + dataset.loc[i,'EstoqueMenos']
        dataset.loc[i,'EstoqueFinal'] = dataset.loc[i,'EstoqueLa'] + dataset.loc[i,'DeltaEstoque']
        dataset.loc[i,'PrecoMedio'] = dataset.loc[i,'EstoqueFinal'] / dataset.loc[i,'QuantidadeFinal']
        #dataset.loc[i,'EstoqueMenos'] = dataset.loc[i,'ValorMovimentacao']  if row['Lancamento'] == 'COMPRA' else 0.00
    
    
    else:
        dataset.loc[i,'QuantidadeLa'] = dataset.loc[i-1,'QuantidadeFinal']
        dataset.loc[i,'QuantidadeFinal'] = dataset.loc[i,'QuantidadeLa'] + dataset.loc[i,'QuantidadeMovimentacao']
        dataset.loc[i,'EstoqueLa'] = dataset.loc[i-1,'EstoqueFinal']
        dataset.loc[i,'EstoqueMais'] = dataset.loc[i,'ValorMovimentacao']  if row['Lancamento'] == 'COMPRA' else 0

        if dataset.loc[i,'QuantidadeLa'] == 0:
            dataset.loc[i,'EstoqueMenos'] = 0
        elif row['Lancamento'] == 'COMPRA':
            dataset.loc[i,'EstoqueMenos'] = 0
        elif row['Lancamento'] == 'VENDA':
            dataset.loc[i,'EstoqueMenos'] = dataset.loc[i,'QuantidadeMovimentacao']*(dataset.loc[i,'EstoqueLa']/dataset.loc[i,'QuantidadeLa'])
        
        
        dataset.loc[i,'DeltaEstoque'] = dataset.loc[i,'EstoqueMais'] + dataset.loc[i,'EstoqueMenos']
        dataset.loc[i,'EstoqueFinal'] = dataset.loc[i,'EstoqueLa'] + dataset.loc[i,'DeltaEstoque']
        dataset.loc[i,'PrecoMedio'] = dataset.loc[i,'EstoqueFinal'] / dataset.loc[i,'QuantidadeFinal']


# Passa colunas para string
print(f"Passa colunas para string....: {datetime.datetime.now()}")
dataset = dataset.astype(str)

print(f"Conexão com o Banco de Dados....: {datetime.datetime.now()}")
db = DBConnectionHandler()
engine = db.get_engine()
tickerDeparaDb = get_depara_database_tickers()

print(f"Enviando dados....: {datetime.datetime.now()}")


dataset.to_sql(
    "Tickers", 
    con = engine,
    if_exists = "replace",
    schema='dbo',   
    index=False,
    chunksize=1000
#    dtype=tickerDeparaDb
)

print(f"Processo Finalizado...: {datetime.datetime.now()}")




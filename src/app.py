import pandas as pd
import datetime
from infra.configs.connection import DBConnectionHandler
from infra.depara.tickers import get_depara_tickers
from infra.entities.tickers import get_depara_database_tickers


print(datetime.datetime.now())

# importa xls
dataset = pd.read_excel('src/files/pm.xlsx')

# renomeia colunas com baseno depara
dataset.rename(columns=get_depara_tickers(), inplace=True)


#Tratando campo ValorMovimentacao
#dataset['ValorMovimentacao'] = dataset['ValorMovimentacao'].replace('[R$ ]', '', regex=True).astype(float)

#cria indice novo agrupando por ticker
dataset['TickerItem'] = dataset.groupby(['Conta']).cumcount()+1

#cria colunas novas conforme EsbocoPm
dataset['QuantidadeLa'] = 0
dataset['QuantidadeFinal'] = 0
dataset['EstoqueLa'] = 0
dataset['EstoqueMais'] = 0
dataset['EstoqueMenos'] = 0
dataset['DeltaEstoque'] = 0
dataset['EstoqueFinal'] = 0
dataset['PrecoMedio'] = 0


#realiza as alterações estoque linha anterior
for i, row in dataset.iterrows():
    
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



db = DBConnectionHandler()
engine = db.get_engine()
tickerDeparaDb = get_depara_database_tickers()


dataset.to_sql(
    "Tickers", 
    con = engine,
    if_exists = "replace",
    schema='dbo',   
    index=False,
    chunksize=1000
    #dtype=tickerDeparaDb
)


#print(dataset)

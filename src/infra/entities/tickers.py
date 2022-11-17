from infra.configs.base import Base
from sqlalchemy import String, Integer, Numeric, DateTime, Numeric

def get_depara_database_tickers():
    return {

            "Conta":Integer,
            "DataCompra":DateTime,
            "Lancamento":String,
            "Mercado":String,
            "Movimentacao":String,
            "Nome":String,
            "QuantidadeMovimentacao":Integer,
            "SubMercado":String,
            "Ticker":String,
            "ValorMovimentacao":Numeric,
            "TickerItem":Integer,
            "QuantidadeLa":Integer,
            "QuantidadeFinal":Integer,
            "EstoqueLa":Numeric,
            "EstoqueMais":Numeric,
            "EstoqueMenos":Numeric,
            "DeltaEstoque":Numeric,
            "EstoqueFinal":Numeric,
            "PrecoMedio":Numeric

        }





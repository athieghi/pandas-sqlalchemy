from infra.configs.base import Base
from sqlalchemy import String, Integer, Float, DateTime

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
            "ValorMovimentacao":Float,
            "TickerItem":Integer,
            "QuantidadeLa":Integer,
            "QuantidadeFinal":Integer,
            "EstoqueLa":Float,
            "EstoqueMais":Float,
            "EstoqueMenos":Float,
            "DeltaEstoque":Float,
            "EstoqueFinal":Float,
            "PrecoMedio":Float

        }





import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import polars as pl
import os
import io
from datetime import datetime, timezone

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger_tuss")
def http_trigger_tuss(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info("--- INICIANDO PIPELINE MONOLÍTICO (SEM FUNÇÕES EXTRAS) ---")

    try:
        # ---------------------------------------------------------
        # 1. CONFIGURAÇÃO E VARIÁVEIS DE AMBIENTE
        # ---------------------------------------------------------
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        
        container_main = os.environ.get("CONTAINER_MAIN_NAME")
        
        blob_file_name_raw = "raw/customers2.csv" 

        # Caminhos do Delta Lake
        url_bronze = os.environ.get("PATH_BRONZE")
        url_silver = os.environ.get("PATH_SILVER")
        url_gold = os.environ.get("PATH_GOLD")

        storage_options = {
            "azure_storage_account_name": account_name,
            "azure_storage_account_key": account_key
        }

        # ---------------------------------------------------------
        # 2. DOWNLOAD (Blob -> Memória)
        # ---------------------------------------------------------
        logging.info(f"--- 1. Baixando {blob_file_name_raw} ---")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string) 
        container_client = blob_service_client.get_container_client(container=container_main)
        blob_client = container_client.get_blob_client(blob=blob_file_name_raw)
        blob_data = blob_client.download_blob().readall()
        file_buffer = io.BytesIO(blob_data)
        
        logging.info("Download OK")

        # ---------------------------------------------------------
        # 3. RAW -> BRONZE
        # ---------------------------------------------------------
        logging.info("--- 2. Processando Bronze ---")
        
        # Lê CSV
        df_raw = pl.read_csv(
            file_buffer, 
            has_header=True,
            separator=',',
            truncate_ragged_lines=True
        )

        # Adiciona Timestamp
        df_raw_w_timestamp = df_raw.with_columns(
            pl.lit(datetime.now(timezone.utc)).alias('data_processamento_utc_raw')
        )

        # Corrige os nomes das colunas
        df_raw_col_fix = df_raw_w_timestamp.select(
            [pl.col(col).alias(col.replace(' ', '_').lower()) for col in df_raw_w_timestamp.columns]
        )
        
        # Escreve na camada bronze
        logging.info(f"Escrevendo Bronze em: {url_bronze}")
        saving_in_bronze_layer = df_raw_col_fix.write_delta(
            target=url_bronze, 
            mode='overwrite', 
            storage_options=storage_options,
            delta_write_options={"schema_mode": "overwrite"}
        )

        # ---------------------------------------------------------
        # 4. BRONZE -> SILVER
        # ---------------------------------------------------------
        logging.info("--- 3. Processando Silver ---")
        df_bronze = pl.read_delta(source=url_bronze, storage_options=storage_options)
        
        # Limpeza de telefones (Regex)
        df_bronze_regex_phone_col = df_bronze.with_columns(
             pl.col('phone').str.replace_all(r'[^0-9]', ''))
    
        # Alterando o schema de uma coluna
        df_bronze_schema_fix = (df_bronze_regex_phone_col.
                                    with_columns(pl.col('registration_date').cast(pl.Date)))

        # Salva na camada silver
        logging.info(f"Escrevendo Silver em: {url_silver}")
        df_bronze_schema_fix.write_delta(
            target=url_silver,
            mode='overwrite',
            storage_options=storage_options,
            delta_write_options={"schema_mode": "overwrite"} 
        )

        # ---------------------------------------------------------
        # 5. SILVER -> GOLD
        # ---------------------------------------------------------
        logging.info("--- 4. Processando Gold ---")

        # NOTA: Lemos o Delta da Silver para garantir a carga em memória
        df_silver = pl.read_delta(source=url_silver, storage_options=storage_options)

        # Agrupa os usuários do Brasil e soma a quantidade de inscrições 
        df_gold = (
            df_silver
            .filter(pl.col("country") == "Brazil")
            .group_by(["registration_date", "country"])
            .agg(
                pl.col("customer_id").count().alias("total_assinaturas_por_dia")
            )
            .sort("total_assinaturas_por_dia", descending=True)
        )

        logging.info(f"Escrevendo Gold em: {url_gold}")
    
        df_gold.write_delta(
            target=url_gold,
            mode='overwrite',
            storage_options=storage_options,
            delta_write_options={"schema_mode": "overwrite"}
        )

        # ---------------------------------------------------------
        # 6. RETORNO HTTP
        # ---------------------------------------------------------
        msg_final = f"""
        Pipeline Executado com Sucesso!
        -------------------------------
        Silver (Total Linhas): {df_silver.height}
        Gold (Linhas Agrupadas): {df_gold.height}
        """
        
        logging.info("Pipeline finalizado.")
        return func.HttpResponse(msg_final, status_code=200, mimetype="text/plain")

    except Exception as e:
        logging.error(f"ERRO : {str(e)}")
        return func.HttpResponse(f"ERRO: {str(e)}", status_code=500, mimetype="text/plain")
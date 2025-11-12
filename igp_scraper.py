import requests
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    # URL de la API del IGP
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"
    
     # Headers necesarios para la solicitud
    #headers = {
    #    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0',
    #    'Accept': 'application/json, text/plain, */*',
    #    'Accept-Language': 'es-AR,es;q=0.8,en-US;q=0.5,en;q=0.3',
    #    'Referer': 'https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados',
    #    'Connection': 'keep-alive'
    #}
    
    try:
        # Realizar la solicitud HTTP a la API
        response = requests.get(url)
        #response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': 'Error al acceder a la API del IGP'
            }
        
        # Obtener los datos JSON
        sismos_data = response.json()
        
        if not sismos_data:
            return {
                'statusCode': 404,
                'body': 'No se encontraron datos de sismos'
            }
        
        # Conectar a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaSismosIGP')
        
        # Eliminar todos los elementos de la tabla antes de agregar los nuevos
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'id': each['id']
                    }
                )
        
        # Insertar los nuevos datos usando batch_writer correctamente
        items_guardados = []
        items_procesados = 0
        
        # Procesar en batches de 25 (límite de DynamoDB)
        with table.batch_writer() as batch:
            for idx, sismo in enumerate(sismos_data, start=1):
                try:
                    # Crear item con los campos del sismo
                    item = {
                        'id': str(uuid.uuid4()),
                        'numero': idx,
                        'codigo': sismo.get('codigo', ''),
                        'reporte_acelerometrico_pdf': sismo.get('reporte_acelerometrico_pdf', ''),
                        'idlistasismos': str(sismo.get('idlistasismos', '')),
                        'fecha_local': sismo.get('fecha_local', ''),
                        'hora_local': sismo.get('hora_local', ''),
                        'fecha_utc': sismo.get('fecha_utc', ''),
                        'hora_utc': sismo.get('hora_utc', ''),
                        'latitud': str(sismo.get('latitud', '')),
                        'longitud': str(sismo.get('longitud', '')),
                        'magnitud': str(sismo.get('magnitud', '')),
                        'profundidad': str(sismo.get('profundidad', '')),
                        'referencia': sismo.get('referencia', ''),
                        'referencia2': sismo.get('referencia2', ''),
                        'referencia3': sismo.get('referencia3', ''),
                        'tipomagnitud': sismo.get('tipomagnitud', ''),
                        'mapa': sismo.get('mapa', ''),
                        'informe': sismo.get('informe', ''),
                        'publicado': sismo.get('publicado', ''),
                        'numero_reporte': str(sismo.get('numero_reporte', '')),
                        'id_pdf_tematico': str(sismo.get('id_pdf_tematico', '')),
                        'createdAt': sismo.get('createdAt', ''),
                        'updatedAt': sismo.get('updatedAt', ''),
                        'intensidad': sismo.get('intensidad', '')
                    }
                    
                    # Guardar en DynamoDB usando batch_writer
                    batch.put_item(Item=item)
                    items_guardados.append(item)
                    items_procesados += 1
                    
                except Exception as e:
                    print(f"Error al guardar sismo {idx}: {str(e)}")
                    continue
        
        # Retornar el resultado
        return {
            'statusCode': 200,
            'body': {
                'mensaje': f'Se guardaron {items_procesados} sismos exitosamente de {len(sismos_data)} totales',
                'total_sismos_api': len(sismos_data),
                'total_guardados': items_procesados,
                'muestra_datos': items_guardados[:10]  # Solo primeros 10 para no sobrecargar la respuesta
            }
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error en el procesamiento: {str(e)}'
        }

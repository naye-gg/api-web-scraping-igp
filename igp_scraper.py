import requests
import boto3
import uuid

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
        #response = requests.get(url, headers=headers)
        response = requests.get(url)
        
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
        
        # Insertar los nuevos datos
        items_guardados = []
        for idx, sismo in enumerate(sismos_data, start=1):
            # Crear item con los campos del sismo
            item = {
                'id': str(uuid.uuid4()),
                'numero': idx,
                'fecha_hora': sismo.get('fecha_hora_utc', ''),
                'latitud': str(sismo.get('latitud', '')),
                'longitud': str(sismo.get('longitud', '')),
                'profundidad': str(sismo.get('profundidad', '')),
                'magnitud': str(sismo.get('magnitud', '')),
                'referencia': sismo.get('referencia', ''),
                'fecha_corte': sismo.get('fecha_corte', '')
            }
            
            # Guardar en DynamoDB
            table.put_item(Item=item)
            items_guardados.append(item)
        
        # Retornar el resultado
        return {
            'statusCode': 200,
            'body': {
                'mensaje': f'Se guardaron {len(items_guardados)} sismos exitosamente',
                'total_sismos': len(items_guardados),
                'datos': items_guardados
            }
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error en el procesamiento: {str(e)}'
        }
        

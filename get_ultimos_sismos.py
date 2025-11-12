import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from decimal import Decimal

# Función helper para convertir Decimal a tipos serializables
def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

def lambda_handler(event, context):
    try:

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaSismosIGP')
        
        # Obtener query parameter para cantidad (por default en 10)
        query_params = event.get('queryStringParameters', {}) or {}
        limite = int(query_params.get('limite', 10))
        
        # Escanear toda la tabla
        response = table.scan()
        items = response['Items']
        
        # Continuar escaneando si hay más páginas
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        items_ordenados = sorted(items, key=lambda x: int(x.get('numero', 0)), reverse=True)
        
        ultimos_sismos = items_ordenados[:limite]
        
        # Convertir Decimals a tipos nativos
        ultimos_sismos = decimal_to_native(ultimos_sismos)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'mensaje': f'Últimos {len(ultimos_sismos)} sismos',
                'total': len(ultimos_sismos),
                'sismos': ultimos_sismos
            }, ensure_ascii=False, default=str)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error al obtener sismos: {str(e)}'
            })
        }

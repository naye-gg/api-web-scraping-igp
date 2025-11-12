import boto3
from boto3.dynamodb.conditions import Key, Attr
import json

def lambda_handler(event, context):
    try:
        # Conectamos a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaSismosIGP')
        
        # Obtener query parameter para cantidad (por default: 10)
        query_params = event.get('queryStringParameters', {}) or {}
        limite = int(query_params.get('limite', 10))
        
        response = table.scan()
        items = response['Items']
        
        # Continuamo escaneando si hay más páginas
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        # Ordenar por número (descendente) para obtener los más recientes
        items_ordenados = sorted(items, key=lambda x: int(x.get('numero', 0)), reverse=True)
        
        # Tomar los últimos N sismos
        ultimos_sismos = items_ordenados[:limite]
        
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
            }, ensure_ascii=False)
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

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from pymongo import MongoClient, TEXT


client = MongoClient('mongodb+srv://jenishdhameliya20:sUoK0NLWKhjlpMs2@cluster0.c0t6ocv.mongodb.net/?retryWrites=true&w=majority')
db = client['Billing']
collection = db['billing_report']
collection.create_index([("text", TEXT)])


class ListItems(APIView):
    """
    ClusterName
    month
    Year
    col
    """

    def get(self, request):
        ClusterName = request.GET.get('ClusterName')
        month = request.GET.get('month')
        Year = request.GET.get('Year')
        col = request.GET.get('col')

        filter_criteria = {
            '$and': [
            {'Cluster Name': ClusterName},
            {'Month': month},
            {'Year': Year},
            ],
        }
        projection = {}
        if col == 'all':
            projection = {}

        else:
            if col:
                columns = col.split(',')
                for column in columns:
                    projection[column] = 1
            else:
                return Response({"message":"please enter a column name"}, status=status.HTTP_400_BAD_REQUEST)
            
        results = collection.find(filter_criteria, projection)
        json_results = []
        for result in results:
            _id = result.pop('_id')
            json_results.append(result)
        return Response(json_results, status=status.HTTP_200_OK)
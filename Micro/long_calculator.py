from math import radians, sin, cos, sqrt, atan2


class CalculadoraDistancia:
    def __init__(self):
        self.radio_tierra = 6371000.0  # Radio de la Tierra en metros

    def distancia_haversine_extendida(self, coord1, coord2):
        lat1, lon1, alt1 = coord1
        lat2, lon2, alt2 = coord2

        # Convertir las coordenadas de grados a radianes
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        # Diferencia de coordenadas
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        # Calcular la distancia horizontal utilizando la fórmula de Haversine
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        distancia_horizontal = self.radio_tierra * 2 * atan2(sqrt(a), sqrt(1 - a))

        # Calcular la diferencia de alturas
        
        diferencia_altura = abs(alt2 - alt1)

        # Calcular la distancia en 3D utilizando el teorema de Pitágoras
        distancia_3d = sqrt(distancia_horizontal**2 + diferencia_altura**2)

        return distancia_3d

from datetime import datetime
from math import sqrt
import logging

class HotcellUtils:
    coordinateStep = 0.01

    @staticmethod
    def CalculateCoordinate(inputString: str, coordinateOffset: int) -> int:
        # Configuration variable:
        # Coordinate step is the size of each cell on x and y
        result = 0
        coordinates = inputString.split(",")
        
        if coordinateOffset == 0:
            result = int(float(coordinates[0].replace("(", "")) // HotcellUtils.coordinateStep) 
        elif coordinateOffset == 1:
            result = int(float(coordinates[1].replace(")", "")) // HotcellUtils.coordinateStep)
        # We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
        elif coordinateOffset == 2:
            timestamp = HotcellUtils.timestampParser(inputString)
            result = HotcellUtils.dayOfMonth(timestamp) # Assume every month has 31 days
        return result

    @staticmethod
    def timestampParser(timestampString: str) -> datetime:
        date_format = "%Y-%m-%d %H:%M:%S"
        parsed_date = datetime.strptime(timestampString, date_format)
        return parsed_date

    @staticmethod
    def dayOfYear(timestamp: datetime):# No used
        return (31 * (timestamp.month - 1) + timestamp.day)

    @staticmethod
    def dayOfMonth(timestamp: datetime) -> int:
        return timestamp.day

    @staticmethod
    def calculateGScore(wij_xj: int, num_neighbors: int, numCells: int, mean: float, std_x: float) -> float:
        result = 0
        try:
            numerator = wij_xj - (mean * num_neighbors)
            denominator = std_x * sqrt(((numCells * num_neighbors) - (num_neighbors ** 2)) / (numCells - 1))
            result = numerator / denominator
        except Exception as ex:
            logging.error(f'{type(ex)}: {ex}')
        return result

    @staticmethod
    def calculateNeighbors(inputX: int, inputY: int, inputZ: int, minX: int, minY: int, minZ: int, maxX: int, maxY: int, maxZ: int) -> int:
        neighbour_check = 0
        # Total 9*3 = 27 cell neighbours including self cell
        total_neighbours = 26
        if inputX == minX or inputX == maxX:
            neighbour_check += 1
        if inputY == minY or inputY == maxY:
            neighbour_check += 1
        if inputZ == minZ or inputZ == maxZ:
            neighbour_check += 1

        missing_neighbours = {0: 0, 1: 9, 2: 15, 3: 19}.get(neighbour_check, 0)
        return total_neighbours - missing_neighbours
    
# if __name__ == '__main__':

    ## TestCase CalculateCoordinate
    # HotcellUtils.CalculateCoordinate('2009-01-23 22:16:00', 2)

    ## TestCase calculateNeighbors
    # X, Y, Z = -7398,4076, 23
    # minX = -74.50 / HotcellUtils.coordinateStep
    # maxX = -73.70/ HotcellUtils.coordinateStep
    # minY = 40.50 / HotcellUtils.coordinateStep
    # maxY = 40.90 / HotcellUtils.coordinateStep
    # minZ, maxZ = 1, 31
    # HotcellUtils.calculateNeighbors(X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ)

    ## TestCase calculateGScore
    # HotcellUtils.calculateGScore()
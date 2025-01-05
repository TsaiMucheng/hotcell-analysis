import logging
class HotzoneUtils:

    # @staticmethod

    def ST_Contains(query_rectangle: str, point_string: str) -> bool:
        result = False
        try:
            rect_coords = list(map(float, query_rectangle.split(",")))
            point_coords = list(map(float, point_string.split(",")))

            rect_x1, rect_y1, rect_x2, rect_y2 = rect_coords
            point_x, point_y = point_coords

            # Check if the point is inside the queryRectangle
            lower_x = min(rect_x1, rect_x2)
            upper_x = max(rect_x1, rect_x2)
            lower_y = min(rect_y1, rect_y2)
            upper_y = max(rect_y1, rect_y2)
            # logging.debug(lower_x, lower_y, upper_x, upper_y)
            result = (lower_x <= point_x <= upper_x) and (lower_y <= point_y <= upper_y)
        except Exception as ex:
            logging.error(f'{type(ex)}: {ex}')
        finally:
            return result

    
# if __name__ == '__main__':
    ## TestCase ST_Contains
    # query_rectangle = '-73.995447,40.697455,-73.979056,40.705968'# _c0  
    # point_string = '-73.993804999999995,40.751818'# _c5  
    # HotzoneUtils.ST_Contains(query_rectangle, point_string)

    
    ## TestCase ST_Contains with csv
    # query_rectangle = '-74.189999,40.671001,-74.153071,40.707982'
    # import pandas as pd
    # point_df = pd.read_csv(r'pointDf.csv')
    # rectangle_df = pd.read_csv(r'rectangleDf.csv')
    # for _, recs in rectangle_df.iterrows():
    #     query_rectangle = recs[0]
    #     for _, rows in point_df.iterrows():
    #         # print(type(rows), rows[0])
    #         point_string = rows[0]
    #         result = HotzoneUtils.ST_Contains(query_rectangle, point_string)
    #         if type(result).__name__ != 'bool':
    #             print('error')
    #         if result:
    #             print(point_string.split(','))
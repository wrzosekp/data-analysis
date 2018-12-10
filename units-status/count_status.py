from database_connection import Database
import pandas as pd
import numpy as np
import sys
"""
count_status.py [start_date] [end_date] ([city_name])
Calculates statuses of locations/ location units / proposals for each day for given period.
If city is defined, returns statistics only for the given city.

(mandatory)[start_date] format: YYYY-MM-DD
(mandatory)[end_date] format: YYYY-MM-DD
(optional) [city] format: 'string'
"""


def get_loc_proposals(database, city=None):
    sql = """
    select lp.id, lp.created_time, lp.last_modified_time, lp.status 
    from location_proposal lp
    left join city c on lp.city_id = c.id
    where lp.type = 'CREATE_LOCATION'"""
    if city:
        sql += """
        and c.name = '%s'
        """ % city
    data = database.run_query(sql)
    return data


def get_lus_proposals(database, city=None):
    sql = """
    select distinct lp.id, lp.created_time, lp.last_modified_time, lp.status 
    from location_proposal lp
    left join location l on lp.location_id = l.id
    left join city c on l.city_id = c.id
    left join location_proposal lp2 on lp.create_new_location_proposal_id = lp2.id
    left join city c2 on lp2.city_id = c2.id
    where lp.type = 'CREATE_LOCATION_UNIT'
    """
    if city:
        sql += """
        and (c.name = '%s' or c2.name = '%s')
        """ % (city, city)
    data = database.run_query(sql)
    return data


def get_locations(database, city=None):
    sql = """
    select l.id, l.created_time, l.updated_time, l.status 
    from location l
    """
    if city:
        sql += """
        left join city c on l.city_id = c.id
        where c.name = '%s'
        """ % city
    data = database.run_query(sql)
    return data


def get_units(database, city=None):
    sql = """
    select lu.id, lu.created_time, lu.updated_time, lu.status, lu.to_scan
    from location_unit lu
    """
    if city:
        sql += """
        left join location l on lu.location_id = l.id
        left join city c on l.city_id = c.id
        where c.name = '%s'
        """ % city

    data = database.run_query(sql)
    return data


def check_spots(database):
    sql = """
    select s.id, s.location_unit_id, s.create_new_location_unit_proposal_id, s.scanning_time, s.last_modified_time
    from spot s 
    where s.status in ('ACTIVE', 'NEW')
    """
    data = database.run_query(sql)
    return data


def check_cities(database):
    sql = """
    select distinct c.name as city_name
    from city c
    """
    data = database.run_query(sql)
    return data


def define_conditions(df, day, col1, col2, main_status):
    conditions = [
        df[col1] > day,
        (df[col1] <= day) & (df[col2] > day),
        df[col2] <= day
    ]

    choices = [0, main_status, df['status']]

    return conditions, choices


def manage_spots(df_spots, day):
    col2 = 'last_modified_time'
    col4 = 'scanning_time'

    conditions = [
        df_spots[col4] > day,
        (df_spots[col4] <= day) & (df_spots[col2] > day) & (df_spots['proposal_id'] == 0),
        (df_spots[col4] <= day) & (df_spots[col2] > day) & (df_spots['proposal_id'] != 0),
        (df_spots[col2] <= day) & (df_spots['location_unit_id'] == 0),
        (df_spots[col2] <= day) & (df_spots['location_unit_id'] != 0)
    ]

    choices = [0, df_spots['location_unit_id'], df_spots['proposal_id'], df_spots['proposal_id'],
               df_spots['location_unit_id']]
    choices2 = [0, 'unit', 'proposal', 'proposal', 'unit']

    df_spots['final_unit_id'] = np.select(conditions, choices)
    df_spots['type'] = np.select(conditions, choices2)

    df_units_spots = pd.DataFrame({'count': df_spots.groupby(['final_unit_id'])['id'].size()}).reset_index() 
    df_units_spots = df_units_spots[df_units_spots['final_unit_id'] != 0]
    scanned_units_list = df_units_spots['final_unit_id'].tolist()

    return scanned_units_list


def manage_date(df, day, col1, col2, main_status, df_spots):
    conditions, choices = define_conditions(df, day, col1, col2, main_status)

    df['status_new'] = np.select(conditions, choices)
    df['status_new'].replace('VALIDATION_ERROR', 'NEW')

    df_status = pd.DataFrame({'count': df.groupby(['status_new'])['id'].count()}).reset_index()
    df_status = df_status[df_status['status_new'] != 0]
    df3 = pd.DataFrame(data=[df_status['count'].values], columns=df_status['status_new'].values)

    #count NEW scanned
    if df_spots is not None:
        scanned_lu_list = manage_spots(df_spots, day)
        scanned = df[(df['status_new'] == main_status) & (df['id'].isin(scanned_lu_list))]['id'].count().astype(int)
        col_name = main_status+'_scanned'
        df3[col_name] = scanned

    all = df_status['count'].sum().astype(int)
    weekday = day.strftime("%A")
    df4 = pd.DataFrame(data=[[day, weekday, all]], columns=['date', 'weekday', 'ALL'])
    df4 = pd.concat([df4, df3], axis=1)
    return df4


def count_statuses(df, daterange, col1, col2, main_status, col_final, df_spots=None):
    df_final = pd.DataFrame(columns=col_final)
    
    for single_date in daterange:
        day = single_date.date()

        df3 = manage_date(df, day, col1, col2, main_status, df_spots)
        df_final = df_final.append(df3, sort=False)

    df_final = df_final.fillna(0)
    df_final = df_final.set_index('date')
    
    return df_final


def count_scanned(df_spots, daterange):
    df_final = pd.DataFrame(columns=['date','proposal','unit'])

    for single_date in daterange:
        day = single_date.date()

        df = manage_spots(df_spots, day)

        df_final = df_final.append(df, sort=True)

    df_final = df_final.fillna(0)
    df_final = df_final.set_index('date')

    return df_final


def column_to_date(df, column_name):
    df[column_name] = df[column_name].dt.date


def main(argv):

    if argv == []:  # no arguments selected
        print(__doc__)
        sys.exit()

    if len(argv) < 2:
        print(__doc__)
        sys.exit()

    start_date = argv[0]
    end_date = argv[1]

    db_ssa = Database('SSA')
    print(db_ssa)

    if len(argv) == 3:
        city = argv[2]
        city_list = check_cities(db_ssa)['city_name'].tolist()
        if city not in city_list:
            print('There is NO that city in the database, check city name')
            sys.exit()
    else:
        city = None

    col1 = 'created_time'
    col2 = 'last_modified_time'
    col3 = 'updated_time'
    col4 = 'scanning_time'
    main_status_prop = 'NEW'
    main_status_loc = 'CREATED'
    columns_proposals = ['date', 'weekday','ALL', 'NEW', 'ACCEPTED', 'DECLINED']
    columns = ['date', 'weekday', 'ALL', 'CREATED', 'REMOVED']

    df_loc_prop = get_loc_proposals(db_ssa, city)
    column_to_date(df_loc_prop, col1)
    column_to_date(df_loc_prop, col2)

    df_unit_prop = get_lus_proposals(db_ssa, city)
    column_to_date(df_unit_prop, col1)
    column_to_date(df_unit_prop, col2)

    df_loc = get_locations(db_ssa)
    column_to_date(df_loc, col1)
    column_to_date(df_loc, col3)
    
    df_unit = get_units(db_ssa)
    column_to_date(df_unit, col1)
    column_to_date(df_unit, col3)

    df_spots = check_spots(db_ssa)
    column_to_date(df_spots, col4)
    column_to_date(df_spots, col2)
    df_spots = df_spots.rename(columns={'create_new_location_unit_proposal_id': 'proposal_id'})
    df_spots = df_spots.fillna(0)
    df_spots['location_unit_id'] = df_spots['location_unit_id'].astype(int)  

    try:
        date_range = pd.date_range(start_date, end_date)

    except ValueError:
        print('Problem with date, check format of chosen dates')
        sys.exit()

    df_l_prop = count_statuses(df_loc_prop, date_range, col1, col2, main_status_prop, columns_proposals)
    df_lu_prop = count_statuses(df_unit_prop, date_range, col1, col2, main_status_prop, columns_proposals, df_spots)

    df_l = count_statuses(df_loc, date_range, col1, col3, main_status_loc, columns)
    df_lu = count_statuses(df_unit, date_range, col1, col3, main_status_loc, columns, df_spots)

    df_lu['CREATED_to_scan'] = df_lu['CREATED'] - df_lu['CREATED_scanned']

    if city is not None:
        writer = pd.ExcelWriter('status_%s_%s_%s.xlsx' % (city, start_date, end_date))
    else:
        writer = pd.ExcelWriter('status_%s_%s.xlsx' % (start_date, end_date))

    df_l_prop.to_excel(writer, 'location_proposals')
    df_lu_prop.to_excel(writer, 'location_unit_proposals')
    df_l.to_excel(writer, 'locations')
    df_lu.to_excel(writer, 'location_units')
    writer.save()


if __name__ == '__main__':
    main(sys.argv[1:])

def insert_user(phone_u, account, tarif, basic_info):

    cql = '''
        BEGIN BATCH
        INSERT INTO mobile.user (phone_u, account, tarif, info) VALUES (
  '%s',%s, %s, {name : '%s', surname : '%s', age : %s}) IF NOT EXISTS;
        APPLY BATCH;
    ''' % (phone_u[0], account[0], tarif[0], basic_info['name'][0], basic_info['surname'][0], basic_info['age'])
    return cql


def insert_talk(id, phone_u, time):
    cql = '''
       INSERT INTO mobile.talk (id, phone, time) VALUES (
  %s, '%s', '%s'); 
    ''' % (id, phone_u[0], time)

    return cql


def buy_paper(email, company, count):
    cql = '''
        INSERT INTO trading.valuable_paper (email, name, count, side) VALUES (
  '%s', '%s', %s, '%s'); 
    ''' % (email, company, count, 'buy')

    return cql


def sell_paper(email, company, count):
    cql = '''
        INSERT INTO trading.valuable_paper (email, name, count, side) VALUES (
  '%s', '%s', %s, '%s'); 
    ''' % (email, company, count, 'sell')

    return cql

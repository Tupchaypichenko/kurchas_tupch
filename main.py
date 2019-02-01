from cassandra.cluster import Cluster


from flask import Flask, request
from flask import render_template

from cassandra_query import insert_user, insert_talk

cluster = Cluster(['127.0.0.1'])

session = cluster.connect('mobile')
app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/add_user', methods=['POST', 'GET'])
def add_user():
    if request.method == 'POST':
        result = dict(request.form)
        print(result)

        phone_u = result.pop('phone_u')
        print(phone_u)
        account = result.pop('account')
        tarif = result.pop('tarif')
        result['age'] = int(result['age'][0])
        basic_info = result
        s = '''SELECT * FROM USER WHERE phone_u='%s' ''' % phone_u[0]

        user = session.execute(s)
        try:
            user = user[0]
        except:
            session.execute(insert_user(phone_u, account, tarif, basic_info))
            return render_template('add_user.html')

        else:
            return 'User already exist'



    return render_template('add_user.html')


@app.route('/add_tarif', methods=['POST', 'GET'])
def add_tarif():
    if request.method == 'POST':

        result = dict(request.form)
        phone_u = result.pop('phone_u')
        tarif = result.pop('tarif')
        session.execute('''
                BEGIN BATCH
                UPDATE mobile.user 
                SET tarif=%s 
                WHERE phone_u='%s';
                APPLY BATCH; ''' % (tarif[0], phone_u[0]))

    return render_template('add_tarif.html')


@app.route('/add_account', methods=['POST', 'GET'])
def add_account():
    if request.method == 'POST':

        result = dict(request.form)
        phone_u = result.pop('phone_u')
        s = session.execute('''SELECT account FROM USER WHERE phone_u='%s' ''' % phone_u[0])
        account = result.pop('account')
        session.execute('''
                BEGIN BATCH
                UPDATE mobile.user 
                SET account=%s 
                WHERE phone_u='%s';
                APPLY BATCH; ''' % (int(s.current_rows.pop(0)[0])+int(account[0]), phone_u[0]))

    return render_template('add_account.html')


@app.route('/add_talk', methods=['POST', 'GET'])
def add_talk():
    if request.method == 'POST':

        result = dict(request.form)
        phone_u = result.pop('phone_u')
        s = session.execute('''SELECT max(id) FROM TALK ''')
        time = int(result.pop('time')[0])


        a = session.execute('''SELECT account FROM USER  WHERE phone_u='%s' ''' % phone_u[0])
        b = session.execute('''SELECT tarif FROM USER  WHERE phone_u='%s' ''' % phone_u[0])
        c=int(a.current_rows.pop(0)[0])-int(b.current_rows.pop(0)[0])*time
        print(c)
        session.execute('''
                        BEGIN BATCH
                        UPDATE mobile.user 
                        SET account=%s 
                        WHERE phone_u='%s';
                        APPLY BATCH; ''' % (c, phone_u[0]))

        session.execute(insert_talk(int(s.current_rows.pop(0)[0])+1,phone_u, time))

    return render_template('add_talk.html')


@app.route('/all_users', methods=['GET'])
def all_users():
    users = session.execute('SELECT * FROM USER')
    return render_template('all_users.html', users=users)


@app.route('/all_talk', methods=['GET'])
def all_talk():
    talks = session.execute('SELECT * FROM TALK')
    return render_template('all_talk.html', talks=talks)





if __name__ == '__main__':
    app.run()

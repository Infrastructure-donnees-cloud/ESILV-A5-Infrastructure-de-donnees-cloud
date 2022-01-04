from flask import render_template, flash, redirect, url_for, request, session
from app import app
from app.forms import LoginForm


from flask_login import current_user, login_user, logout_user, login_required
from app.models import User

from app import db
from app.forms import RegistrationForm

import pygal
from pygal.style import Style

from werkzeug.urls import url_parse



import mongo_queries


import pandas as pd

import plotly
import plotly.express as px
import json




@app.route('/')


@app.route('/index')
@login_required
def index():

    queries = [
        {
            "number" : 1,
            "body": "All members who took a loan in XX, according to category Y.",
            "access_type":0
        },
        {
            "number" : 2,
            "body": "List of payments made by employees of company XX and whose credit provider resides in Savannah.",
            "access_type":0
        },
        {
            "number" : 3,
            "body": "List of members who have contracted a category XX loan and who live in a street with the word YY in it.",
            "access_type":0
        },
        {
            "number" : 4,
            "body": "Member with a capital greater than XX $, having taken a loan from a provider YY, and whose telephone number and the one of the provider end with the same numbers.",
            "access_type":0
        },
        {
            "number" : 5,
            "body": "Corporation whose employees are indebted and must repay their loans as quickly as possible.",
            "access_type":1
        },
        {
            "number" : 6,
            "body": "By category, name of the provider that provides the most liquidity.",
            "access_type":1
        },
        {
            "number" : 7,
            "body": "Average time required for a member to repay a loan (already paid) by range of credit contracted. TAKE TIMES TO COMPUTE",
            "access_type":1
        },
        {
            "number" : 8,
            "body": "Average interest rate of each provider, by category.",
            "access_type":1
        },
    ]

    return render_template('index.html', title='Home Page', queries=queries)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(username=form.username.data, email=form.email.data, access_type=form.access_type.data)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Congratulations, you are now a registered user!')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user, remember=form.remember_me.data)
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        return redirect(next_page)
    return render_template('login.html', title='Sign In', form=form)


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('index'))


@app.route('/q1', methods =["GET", "POST"])
def q1():

    if request.method == "POST":
        
       category = request.form.get("category")
       year = request.form.get("year")

       res = mongo_queries.query_1(category, year)
       
       return render_template('q1.html', title='Query 1', res=res)
    
    return render_template("q1.html")


@app.route('/q2', methods =["GET", "POST"])
def q2():

    if request.method == "POST":
       
       corp_name = request.form.get("corp")
       #city = request.form.get("city")

       res = mongo_queries.query_2(corp_name)
       
       return render_template('q2.html', title='Query 2', res=res)
    
    return render_template("q2.html")


@app.route('/q3', methods =["GET", "POST"])
def q3():

    if request.method == "POST":
       
       category = request.form.get("category")
       street = request.form.get("street")

       res = mongo_queries.query_3(category, street)
       
       return render_template('q3.html', title='Query 3', res=res)
    
    return render_template("q3.html")


@app.route('/q4', methods =["GET", "POST"])
def q4():

    if request.method == "POST":
       
       capital = request.form.get("capital")
       provider = request.form.get("provider")

       res = mongo_queries.query_4(capital, provider)
       
       return render_template('q4.html', title='Query 4', res=res)
    
    return render_template("q4.html")


@app.route('/q5')
def q5():

    if current_user.access_type>=1:
   
        res = mongo_queries.query_5()    
        return render_template('q5.html', title='Query 5', res=res)

    else:
        return render_template('denied.html')

@app.route('/q6')   
def q6():

    if current_user.access_type>=1:

        res = mongo_queries.query_6()

        #Collect all the informations to plot

        data = pd.json_normalize(res)

        fig = px.bar(data, x='_id', y='maxi.amount', color='maxi.provider_name', barmode='group', title = 'By category, name of the provider that provides the most liquidity.',
        
        labels={
                        "_id": "Credit category",
                        "maxi.provider_name": "Provider name",
                        "maxi.amount": "Amount provided"
                    }
        
        )

        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


        return render_template('q6.html', graphJSON = graphJSON, res=res)

    else:
        return render_template('denied.html')


@app.route('/q7')   
def q7():

    if current_user.access_type>=1:

        res = mongo_queries.query_7()

        for elem in res:
            elem['average_repayment_time'] = abs(elem['average_repayment_time'])

        #Collect all the informations to plot

        data = pd.json_normalize(res)
        fig = px.bar(data, x='_id', y='average_repayment_time', barmode='group', title = 'Average time required for a member to repay a loan (already paid) by range of credit contracted.',
        
        labels={
                        "_id": "Credit range in $",
                        "average_repayment_time": "Average time in days"
                    }
        
        )

        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return render_template('q7.html', title='Query 7', res=res, graphJSON = graphJSON)

    else:
        return render_template('denied.html')


@app.route('/q8')   
def q8():

    if current_user.access_type>=1:

        res = mongo_queries.query_8()
        #Collect all the informations to plot

        data = pd.json_normalize(res)
        fig = px.bar(data, x='category', y='monthly_rate', color='provider_no', title = 'Average interest rate of each provider, by category.',
        
        labels={
                        "category": "Credit loan category",
                        "monthly_rate": "Monthly rate (%)"
                    }
        
        )

        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return render_template('q8.html', title='Query 8', res=res, graphJSON = graphJSON)

    else:
        return render_template('denied.html')


@app.route('/admin')   
def admin():

    if current_user.access_type>=2:

        collections = mongo_queries.get_list_collections()
        number_objects = mongo_queries.get_number_objects()
        avg_object_size = mongo_queries.get_avg_object_size()
        data_size = mongo_queries.get_data_size()
        indexes = mongo_queries.get_indexes()
        storage_size = mongo_queries.get_storage_size()
        num_collections = mongo_queries.get_number_collection()


        return render_template('admin.html', collections=collections, number_objects=number_objects,
        avg_object_size=avg_object_size, data_size = data_size, indexes = indexes,
        storage_size = storage_size, num_collections=num_collections)

    else:
        return render_template('denied.html')
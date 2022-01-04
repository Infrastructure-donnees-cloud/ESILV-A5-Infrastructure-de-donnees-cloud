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

@app.route('/')


@app.route('/index')
@login_required
def index():


    queries = [
        {
            "number" : 1,
            "body": "All members who took a loan in 2021, according to category Trip."
        },
        {
            "number" : 2,
            "body": "List of payments made by employees of company XX and whose credit provider resides in city YY."
        },
        {
            "number" : 3,
            "body": "List of members who have contracted a category XX loan and who live in a street with the word YY in it."
        },
        {
            "number" : 4,
            "body": "Member with a capital greater than XX $, having taken a loan from a provider YY, and whose telephone number and the one of the provider end with the same numbers."
        },
    ]
    return render_template('index.html', title='Home Page', queries=queries)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(username=form.username.data, email=form.email.data)
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



@app.route('/bar_route')   
@login_required
def bar_route():
    
    try:

        bar_chart = pygal.Bar()
        bar_chart.title = 'Browser usage evolution (in %)'
        bar_chart.x_labels = map(str, range(2002, 2013))
        bar_chart.add('Firefox', [None, None, 0, 16.6,   25,   31, 36.4, 45.5, 46.3, 42.8, 37.1])
        bar_chart.add('Chrome',  [None, None, None, None, None, None,    0,  3.9, 10.8, 23.8, 35.3])
        bar_chart.add('IE',      [85.8, 84.6, 84.7, 74.5,   66, 58.6, 54.7, 44.8, 36.2, 26.6, 20.1])
        bar_chart.add('Others',  [14.2, 15.4, 15.3,  8.9,    9, 10.4,  8.9,  5.8,  6.7,  6.8,  7.5])
        barchart_data=bar_chart.render_data_uri()
        return render_template('barchart.html',barchart_data=barchart_data)

    except Exception:
        return "error"


@app.route('/q1')   
def q1():
    
    
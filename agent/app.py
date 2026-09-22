from flask import Flask
from agent.interactor.trigger_calculation import trigger_calculation_bp
from agent.interactor.stats import stats_blueprint
from agent.calculation.api import calculation_blueprint
from agent.interactor.csv_export import csv_export_bp
from agent.interactor.visualisation import visualisation_bp

app = Flask(__name__)
app.register_blueprint(trigger_calculation_bp)
app.register_blueprint(calculation_blueprint)
app.register_blueprint(csv_export_bp)
app.register_blueprint(stats_blueprint)
app.register_blueprint(visualisation_bp)

if __name__ == "__main__":
    app.run()

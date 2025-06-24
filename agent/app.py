from flask import Flask
from agent.interactor.trigger_calculation import trigger_calculation_bp
from agent.calculation.api import calculation_blueprint
from agent.interactor.generate_results import generate_results_bp

app = Flask(__name__)
app.register_blueprint(trigger_calculation_bp)
app.register_blueprint(calculation_blueprint)
app.register_blueprint(generate_results_bp)

if __name__ == "__main__":
    app.run()

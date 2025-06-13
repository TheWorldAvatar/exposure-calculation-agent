from flask import Flask
from agent.interactor.trigger_calculation import trigger_calculation_bp
from agent.calculation.api import calculation_blueprint

app = Flask(__name__)
app.register_blueprint(trigger_calculation_bp)
app.register_blueprint(calculation_blueprint)

if __name__ == "__main__":
    app.run()

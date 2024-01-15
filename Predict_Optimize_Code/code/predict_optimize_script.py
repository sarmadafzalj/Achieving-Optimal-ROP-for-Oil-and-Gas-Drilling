from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from pyswarm import pso

# Initialize Flask app
app = Flask(__name__)

# Enable CORS for all routes
CORS(app)

# Load the model and scaler
with open('grid_model.pkl', 'rb') as file:
    ml_model = pickle.load(file)

with open('scaler.pkl', 'rb') as file:
    scaler = pickle.load(file)

# Function to make predictions using the 'grid' model
def predict_rop(model, data):
    # Scale the data
    data = scaler.transform(data)
    return model.predict(data)

# Function to optimize parameters using PSO and the 'grid' model
def optimize_parameters(model, constants):
    # Objective function for PSO
    def objective_function(params, model, constants):
        WOB, RPM = params
        data = pd.DataFrame({
            'Depth': constants['Depth'], 
            'WOB': WOB, 
            'SURF_RPM': RPM,
            'PHIF': constants['PHIF'], 
            'VSH': constants['VSH'],
            'SW': constants['SW'], 
            'KLOGH': constants['KLOGH']
        }, index=[0])
        predicted_rop = predict_rop(model, data)
        return -predicted_rop  # Negative sign for maximization in PSO

    # Define bounds for WOB and RPM
    lb = [15000, 1.4]  # Lower bounds
    ub = [60000, 2.8]  # Upper bounds

    # Running PSO
    best_params, _ = pso(objective_function, lb, ub, args=(model, constants))
    return best_params

# Define route for prediction
@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json(force=True)
    if "ROP_AVG" in data:
        del data["ROP_AVG"]
    prediction = predict_rop(ml_model, pd.DataFrame([data]))
    return jsonify({'prediction': prediction.tolist()[0]})

# Define route for optimization
@app.route('/optimize', methods=['POST'])
def optimize():
    constants = request.get_json(force=True)
    del constants["ROP_AVG"], constants["WOB"], constants["SURF_RPM"]
    opt1, opt2 = optimize_parameters(ml_model, constants)
    return jsonify({'WOB': opt1, 'RPM': opt2})

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)




# Orata
[Sergius Orata](https://en.wikipedia.org/wiki/Sergius_Orata) was an Ancient Roman who was a successful merchant, inventor and hydraulic engineer.

This project is the monitoring of a Telegram channel and and executing trades in crypto currencies on the Coinbase exchange.

The various commands are recieved from algo trading strategies hosted and mainatined in another state.

## Supported cammands
### Buy 
Executes BUY order of TRADE_UNIT
  * Parameters: 
    1. Account of model index
    2. wait time until reverse trade
    3. (not yet) percentage of UNIT_TRADE
 * Returns:
    1. Returns the Model balance after trade
### Sell
Executes SELL order of TRADE_UNIT/BTC_rate
* Parameters: 
    1. Account of model index
    2. wait time until reverse trade
    3. (not yet) percentage of UNIT_TRADE
* Returns:
    1. Returns the Model balance after trade
### BAL
* Parameters: 
    1. Account of model index
* Returns:
    1. Returns the Model balance after trade
### Reset
Return the model account balance to DEFAULT_USD and DEFAULT_BTC
* Parameters: 
    1. Account of model index
* Returns:
    1. Returns the Model balance after trade
### Chart
Return the model Chart of requested model. 
* Parameters: 
    1. Account of model index
    2. (optional) Second model index
    3. (optional) "z" for zero to first value
* Returns:
    1. Returns zip file of HTML page of a chart with the performance of the last 24 hours.
    - If 1 index supplied you get a stacked graph of USD & BTC$
    - If 2 indices supplied The Total USD against BTC rate returned


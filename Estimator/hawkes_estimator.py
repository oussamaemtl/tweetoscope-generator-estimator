## Internal libraries
import argparse, json
from kafka import KafkaProducer, KafkaConsumer
import numpy as np
##External libraries
import logger as Logger
from labwork_functions import compute_MAP, prediction

## functions

## main


def main():

    ## Logger creation 

    logger = Logger.get_logger('Hawkes Estimator', broker_list='localhost:9092', debug=True)  # the source string (here 'my-node') helps to identify
                                                                                 # in the logger terminal the source that emitted a log message.
    
    ## Topics

    input_topic="cascade_series"
    output_topic="cascade_properties"

    ## Parser setting up

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--broker-list', type=str, required=True, help="the broker list")
    args = parser.parse_args()  # Parse arguments
    
    ## Consumer of cascade_series
    
    consumer = KafkaConsumer(input_topic,                   # Topic name
    bootstrap_servers = args.broker_list,                        # List of brokers passed from the command line
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # How to deserialize the value from a binary buffer
    key_deserializer= lambda v: v.decode()                       # How to deserialize the key (if any)
    )

    ## Producer of cascade_properties

    producer = KafkaProducer(
    bootstrap_servers = args.broker_list,                     # List of brokers passed from the command line
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # How to serialize the value to a binary buffer
    key_serializer=str.encode                                 # How to serialize the key
    )


    ## Data processing and estimation using MAP estimator

    ### Prior model parameters : Negative Power law

    alpha=  2.4
    mu=10
    ### Magnitudes list
    magnitudes=[]

    ### Getting data from the input topic
    for msg in consumer:        # Blocking call waiting for a new message

        logger.info("-------------------------------------------------------------")
        logger.info("-------------------------------------------------------------")
        logger.info("Catch a new cascade")

        ### checking that we have a serie for the type
        if(msg.value['type']!='serie'):
            logger.critical("The cascade is not of type serie. Please Check the cascade type again")
            break
        ### retrieving information from the a new message

        T_obs=msg.value['T_obs']
        cid=msg.value['cid']
        message=msg.value['msg']
        history=np.asarray(msg.value['tweets'])
        n_obs=history.shape[0]

        logger.info(f"Processing a cascade with the identifier {cid} and a windows size of  {T_obs} second(s)")

        ### Update magnitudes list
        magnitudes.append(history[:,1])

        ### Reset the starting to 0.0 second
        history[:,0] = history[:,0] - history[0,0]

        
        logger.info(f"Pamareters estimation with MAP")

        ## Loglikelihood , parameters and the predicted number of futur retweets

        LL,params = compute_MAP(history, T_obs, alpha, mu)
        n_supp = round(prediction(params, history, alpha, mu, T_obs) - n_obs)

        logger.info(f"Ready to send a message to the cascade_properties topic")
        
        output_message = {
        'type': 'parameters',
        'cid': cid,
        'msg': message,
        'n_obs': n_obs,
        'n_supp':n_supp,
        'params': list(params)}  # arrays are not JSON serializable

        # Send the message to the cascade_properties topic

        producer.send(output_topic, key = msg.key, value = output_message) 

        logger.info("Message successfully sent to the cascade_properties topic")
    
    # Flush: force purging intermediate buffers before leaving
    producer.flush()     


if __name__=="__main__":
    main()
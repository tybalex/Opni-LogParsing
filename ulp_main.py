from grouping import log_parsing

def generate_file(dataset_name, format_t, is_logpai: bool = True): 
    groundtruth_file = "logs/" + dataset_name + "/" + dataset_name + "_2k.log_templates.csv"
    predict_file = log_parsing(dataset_name,format_t,0.65, is_logpai)
    # measure_accuracy(groundtruth_file, predict_file)
    return 0

generate_file("rancher_training_dataset.txt", "<Content>", is_logpai = False)
# generate_file("finalized_longhorn_training_dataset.txt", "<Content>", is_logpai = False)

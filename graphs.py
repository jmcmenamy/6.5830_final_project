import matplotlib.pyplot as plt
import seaborn as sns

METHODS = ["reference", "metadata", "stats", "contiguous", "stratified"]

def get_absolute_percent_error(reference, result):
    return abs((result - reference) / reference) * 100

def get_mean_absolute_percent_error(references, results):
    absolute_percent_errors = []
    for reference, result in zip(references, results):
        absolute_percent_errors.append(get_absolute_percent_error(reference, result))
    return sum(absolute_percent_errors) / len(absolute_percent_errors)

def get_trial_means(list_of_lists):
    num_indices = len(list_of_lists[0])
    sums = [0] * num_indices
    counts = [0] * num_indices
    for lst in list_of_lists:
        for i, value in enumerate(lst):
            sums[i] += value
            counts[i] += 1

    means = [sums[i] / counts[i] for i in range(num_indices)]
    return means

def scatterplot(datapoints):
    sns.set(style="whitegrid", context="talk")

    plt.rcParams["axes.facecolor"] = "#fffcf5ff"  # Background of the plot
    plt.rcParams["figure.facecolor"] = "#fffcf5ff"  # Background of the figure

    plt.figure(figsize=(10, 6))
    plt.xscale('log')
    # plt.yscale('log')
    plt.ylim(-10, 100)

    method_colors = {
        "reference": "black",
        "metadata": "blue",
        "stats": "orange",
        "contiguous": "red",
        "stratified": "green",
    }

    for method in datapoints:
        x_values, y_values = datapoints[method]
        plt.scatter(x_values, y_values, label=method, s=100, color=method_colors[method], alpha=0.7, edgecolor="k")

    plt.xlabel("log(Query Execution Time) (ms)", fontsize=14)
    plt.ylabel("Mean Absolute Percent Error", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12, loc="upper right")

    plt.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.7)
    plt.tight_layout()

    plt.show()

### AVG

# Values are ordered alphabetically by l_returnflag (A, N, R)
avg_query_values = {
    "reference": [[25.522005853257337, 25.502204115048958, 25.50579361269077],
                   [25.522005853257337, 25.502204115048958, 25.50579361269077],
                   [25.522005853257337, 25.502204115048958, 25.50579361269077]],
    "metadata": [[25.21949965729952, 25.496032740332414, 25.302313445816665],
                 [25.592077464788733, 25.36082130039229, 25.6208241478718],
                 [25.666090563428966, 25.55482254697286, 25.375884077971364]],
    "stats": [[25.09815791749546, 24.946771233105288, 24.974009152922893],
              [24.987687978190134, 24.95052754008474, 24.63173678348042],
              [25.133877901977645, 24.839336334834083, 24.916333275352237]],
    "contiguous": [[25.424655172413793, 25.6054257095186, 25.59276562859122],
                   [25.424655172413793, 25.6054257095186, 25.59276562859122],
                   [25.424655172413793, 25.6054257095186, 25.59276562859122]],
    "stratified": [[25.443197513812155, 25.18424801005446, 25.8290173212142],
                   [25.32651991614256, 25.5755206351832, 25.613964826745605],
                   [25.558339052848318, 25.73462538803591, 25.59318533815178]]
}

avg_query_times = {
    "reference": [24820.671583, 24820.671583, 24820.671583],
    "metadata": [424.945792, 408.839916, 421.319584],
    "stats": [447.852709, 439.231792, 444.032125],
    "contiguous": [108.084209, 117.989458, 105.85225],
    "stratified": [662.376417, 649.112458, 665.713958]
}

### COUNT

# Values are ordered alphabetically by l_returnflag (A, N, R)
count_query_values = {
    "reference": [[1478493, 3043852, 1478870],
                  [1478493, 3043852, 1478870],
                  [1478493, 3043852, 1478870]],
    "metadata": [[574304, 1201809, 579704],
                 [588704, 1198109, 569004],
                 [567604, 1198609, 589604]],
    "stats": [[1502086, 3031940, 1467187],
              [1455978, 3057287, 1487948],
              [1484254, 3045696, 1471262]],
    "contiguous": [[580004, 1198009, 577804],
                   [580004, 1198009, 577804],
                   [580004, 1198009, 577804]],
    "stratified": [[571504, 1202109, 582204],
                   [583704, 1194109, 578004],
                   [582804, 1190409, 582604]],
}

count_query_times = {
    "reference": [24291.76042, 24291.576042, 24291.576042],
    "metadata": [418.405, 377.804917, 413.180208],
    "stats": [452.437834, 449.137166, 447.749125],
    "contiguous": [106.078792, 105.883333, 115.250083],
    "stratified": [665.335333, 740.38525, 670.608958]
}

### MAX

# Values are ordered alphabetically by l_linestatus (F, O)
max_query_values = {
    "reference": [[104949.5, 104749.5],
                  [104949.5, 104749.5],
                  [104949.5, 104749.5]],
    "metadata": [[1.640168141292686e+09, 1.640168141292686e+09],
                 [1.6336143960174842e+09, 1.6336143960174842e+09],
                 [1.6219032241632411e+09, 1.6219032241632411e+09]],
    "stats": [[108156.44879360525, 108156.44879360525],
              [108156.44879360525, 108156.44879360525],
              [108156.44879360525, 108156.44879360525]],
    "contiguous": [[1.6377867717158139e+09, 1.6377867717158139e+09],
                   [1.6377867717158139e+09, 1.6377867717158139e+09],
                   [1.6377867717158139e+09, 1.6377867717158139e+09]],
    "stratified": [[1.6217830020267758e+09, 1.6217830020267758e+09],
                   [1.6237236864706771e+09 , 1.6237236864706771e+09],
                   [1.6124302766541245e+09, 1.6124302766541245e+09]],
}

max_query_times = {
    "reference": [25166.924459, 25166.924459, 25166.924459],
    "metadata": [381.828625, 383.179291, 383.503333],
    "stats": [443.991333, 454.956667, 461.041083],
    "contiguous": [97.456292, 108.441084, 106.540583],
    "stratified": [640.698458, 654.572792, 654.681542]
}


if __name__ == "__main__":
    # Each method is mapped to a tuple containing two lists: query execution
    # times, mean absolute percent errors.
    datapoints = {method: ([], []) for method in METHODS}
    for method in METHODS:
        # TODO: Simplify
        reference_means = get_trial_means(avg_query_values["reference"])
        method_means = get_trial_means(avg_query_values[method])
        datapoints[method][0].append(sum(avg_query_times[method]) / len(avg_query_times[method]))
        datapoints[method][1].append(get_mean_absolute_percent_error(reference_means, method_means))

        reference_means = get_trial_means(count_query_values["reference"])
        method_means = get_trial_means(count_query_values[method])
        datapoints[method][0].append(sum(count_query_times[method]) / len(count_query_times[method]))
        datapoints[method][1].append(get_mean_absolute_percent_error(reference_means, method_means))

        reference_means = get_trial_means(max_query_values["reference"])
        method_means = get_trial_means(max_query_values[method])
        datapoints[method][0].append(sum(max_query_times[method]) / len(max_query_times[method]))
        datapoints[method][1].append(get_mean_absolute_percent_error(reference_means, method_means))

    scatterplot(datapoints)

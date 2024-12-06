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
    # plt.ylim(-10, 100)
    plt.ylim(-0.1, 0.7)

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

# Values are ordered alphabetically by l_returnflag (A, N, R)
avg_query_values_2 = {
    "reference": [[0.04998529583825443, 0.050001448821967484, 0.050009405829983596],
                  [0.04998529583825443, 0.050001448821967484, 0.050009405829983596],
                  [0.04998529583825443, 0.050001448821967484, 0.050009405829983596]],
    "metadata": [[0.0500069360152592, 0.04958709143566189, 0.050297932001402],
                 [0.049523891077239215,  0.050667945296862485, 0.05016064257028119],
                 [0.0496885274268906, 0.049913151364762015, 0.04978203550711913]],
    "stats": [[0.050326565874728175,  0.04966373598899051, 0.05033443077456033],
              [0.05040795337675488, 0.049872897353067956, 0.05011284348350201],
              [0.049762188885098424, 0.050088655462175995, 0.04971512430939019]],
    "contiguous": [[0.05007758620689649, 0.04976794657762705,  0.049714434060228484],
                   [0.05007758620689649, 0.04976794657762705, 0.049714434060228484],
                   [0.05007758620689649, 0.04976794657762705, 0.049714434060228484]],
    "stratified": [[0.04940753424657535, 0.05006526104417469, 0.050972944849115374],
                   [0.05017416795999316,  0.04995643796598593, 0.050309172105805385],
                   [0.04991952054794509, 0.04992621782510089, 0.049473320670005226]]
}

avg_query_times_2 = {
    "reference": [25056.759958, 25056.759958, 25056.759958],
    "metadata": [484.57175, 470.096125, 420.240667],
    "stats": [445.209583, 467.397333, 468.619667],
    "contiguous": [112.792791, 113.305208, 101.775167],
    "stratified": [668.115334, 643.638834, 657.390167]
}

# Values are ordered alphabetically by l_linestatus (F, O)
avg_query_values_3 = {
    "reference": [[0.039992573969171316,  0.040034382718454706],
                  [0.039992573969171316,  0.040034382718454706],
                  [0.039992573969171316,  0.040034382718454706]],
    "metadata": [[0.04007493826108976,  0.04014134574692996],
                 [0.03956876947696276, 0.03984082156610868],
                 [0.03996502601722933, 0.04013772708069108]],
    "stats": [[0.03989843617201518,  0.03991943690637103],
              [0.04018816517666465, 0.040228561753993496],
              [0.040083751381678884, 0.04023777231498918]],
    "contiguous": [[0.04035306157257199,  0.04016227180527188],
                   [0.04035306157257199, 0.04016227180527188],
                   [0.04035306157257199, 0.04016227180527188]],
    "stratified": [[0.03990930466912824, 0.03996480686695109],
                   [0.0399050551706423, 0.0402637566360478],
                   [0.04020819228165021, 0.040027252597511484]]
}

avg_query_times_3 = {
    "reference": [25165.254417, 25165.254417, 25165.254417],
    "metadata": [416.290917, 412.298625, 394.672334],
    "stats": [452.480167, 443.66875, 450.154083],
    "contiguous": [114.557417, 109.134417, 114.015917],
    "stratified": [661.652291, 629.584416, 656.117875]
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

# Update as we plot points from more queries
values_and_times = [
    (avg_query_values, avg_query_times),
    (avg_query_values_2, avg_query_times_2),
    (avg_query_values_3, avg_query_times_3),
    (count_query_values, count_query_times),
    (max_query_values, max_query_times)
]


if __name__ == "__main__":
    # Each method is mapped to a tuple containing two lists: query execution
    # times, mean absolute percent errors.
    datapoints = {method: ([], []) for method in METHODS}
    for method in METHODS:
        for values, times in values_and_times:
            reference_means = get_trial_means(values["reference"])
            method_means = get_trial_means(values[method])
            datapoints[method][0].append(sum(times[method]) / len(times[method]))
            datapoints[method][1].append(get_mean_absolute_percent_error(reference_means, method_means))

    scatterplot(datapoints)

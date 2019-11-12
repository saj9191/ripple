import matplotlib
import os
import matplotlib.pyplot as plt
#  matplotlib.use('Agg')


def main():

    # TODO: We need a spectral count threshold for a confident (species) identification
    #  For now, exclude files based on this threshold

    # If something is added here, please comment a reason
    datasets_to_exclude = (
        'PXD001873',  # This is SILAC human phospho
        'PXD002801-TMT10',  # This one uses MS3, so exclude
        'PXD003177',  # no data for this one
        'PXD005323'  # This is use of 4 reagents (117, 118, 119, 121) from 8plex and will trick medic
        )

    # Use this to exclude files from data sets for known reasons
    # If something is added here, please comment a reason
    files_to_exclude = ()

    datasets = {
        "ALS_CSF_Biomarker_Study-1530309740015": "normalHuman",
        #  "Coon-HeLa-APD-Data": "normalHuman",  # 2 / 6 files with not hits, others < 25%
        # "Coon-SingleShotFusionYeast": "normalYeast",  # this may be due to different species of "yeast" pombe?
        "DIA-Files": "normalMouse",
        #"Differential_analysis_of_a_synaptic_density_fraction_from_Homer2_knockout_and_wild-type_olfactory_bulb_digests-1534804159922": "normalMouse",  # This dataset has no hits!
        # "Differential_analysis_of_hemolyzed_mouse_plasma-1534802693575": "normalMouse", # No hits
        "Momo_Control_Yeast_DDA": "normalYeast",
        "PES_Unfractionated-1533935400961": "normalRoundworm",
        "PXD001250-Mann_Mouse_Brain_Proteome": "normalMouse",  #  Unusual number of FPs, maybe specific to brain tissue?
        # "PXD001873": "silacLys6Arg6Human", #  SIALC phospho (not parameter set)
        # "PXD002079": "phosphorylationHuman",  # uses reductive dimentylation for isobaric labeling
        # "PXD002098-Mann_SILAC_Human_Cell_Lines": "silacLys8Arg10Human",  # this is super-silac Lys8 only
        # "PXD002801-TMT10": "tmt6Mouse",  # MS3
        # "PXD003177": "itraq8Mouse", # no data
        # "PXD005323": "itraq4Human", # itraq8 reagents used in "itraq4" experiment
        "PXD005709": "normalHuman",
        # "PXD009220": "itraq8Mouse", # Most files have 0 hits
        # "PXD009227": "phosphorylationHuman", # mixed phospho and not phospho
        # "PXD009240": "phosphorylationFruitFly", # mixed phospho and not phospho
        "Unit-Resolution_Test_Files": "normalHuman",
        }

    # check_data(datasets)
    #graph_thresholds(datasets)

    threshold = 0.25
    [correct, unknown, wrong] = calculate_scores(threshold, datasets)

    graph(correct, unknown, wrong, threshold)


def graph_thresholds(datasets):
    fig, ax = plt.subplots()
    thresholds = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7]
    threshold_results = {}

    for threshold in thresholds:
        threshold_results[threshold] = calculate_scores(threshold, datasets)
        print(threshold, threshold_results[threshold])

    tags = ["medic_500", "medic_1000", "medic_2000", "500", "1000", "2000"]
    for tag in tags:
        x_values = []
        y_values = []
        for threshold in thresholds:
            [correct, unknown, wrong] = threshold_results[threshold]
            x_values.append(correct[tag])
            y_values.append(wrong[tag])
        plt.plot(x_values, y_values, marker="x", label=tag)

    plt.ylabel("Wrong Percentage")
    plt.xlabel("Correct Percentage")
    plt.legend()
    plot_name = "data_counts/thresholds.png"
    plt.show()
    print("Plot", plot_name)
    fig.savefig(plot_name)
    plt.close()


def calculate_scores(threshold, datasets):
    scores = {}
    tops = [
        500,
        1000,
        2000,
        ]

    folders = list(datasets.keys())
    folders.sort()
    costs = {"medic_": 0.0, "ppm_": 0.0, "": 0.0}
    for top in tops:
        for medic in ["ppm_", "medic_", ""]:
            total_count = 0
            token = "{0:s}{1:d}".format(medic, top)
            if token not in scores:
                scores[token] = [0, 0, 0]
            for folder in folders:
                count = 0
                bfolder = "data_counts/{0:s}/{1:s}".format(token, folder)
                if os.path.isdir(bfolder):
                    for root, dirs, files in os.walk(bfolder):
                        if len(files) > 0:
                            for file in files:
                                csv_file = "{0:s}/{1:s}".format(root, file)
                                counts = calculate_score(csv_file, datasets[folder], threshold * top)
                                costs[medic] += counts[-1]
                                for i in range(len(counts[:3])):
                                    scores[token][i] += counts[i]
                                count += counts[2]
                    total_count += count
                else:
                    print("Cannot find top", token, "for", folder)
                    pass

    print("")
    correct = {}
    wrong = {}
    unknown = {}
    for top in tops:
        for medic in ["medic_", ""]:
            token = "{0:s}{1:d}".format(medic, top)
            if token in scores:
                [correct_count, unknown_count, total_count] = scores[token]
                wrong_count = total_count - correct_count - unknown_count
                correct[token] = (float(correct_count) / total_count) * 100
                unknown[token] = (float(unknown_count) / total_count) * 100
                wrong[token] = (float(wrong_count) / total_count) * 100
                print("Top", token, "Correct count", correct_count)
                print("Top", token, "Wrong count", wrong_count)
                print("Top", token, "Unknown count", unknown_count)
                print("Top", token, "Total count", total_count)
                print("")

    print("COUNT", count)
    print("PARAMEDIC COST", costs["medic_"] / count)
    print("NORMAL COST", costs[""] / count)
    return [correct, unknown, wrong]


def parse_file(file):
    specie_scores = {}
    f = open(file)
    lines = f.readlines()
    cost = sum(list(map(lambda c: float(c), lines[1].split(",")[1:])))
    lines = lines[1:]
    for line in lines:
        parts = line.strip().split(",")
        assert (len(parts) == 2)
        fasta = parts[0]
        if len(parts[1]) == 0:
            score = 0
        else:
            score = int(parts[1])
        specie_scores[fasta] = score
    return [specie_scores, cost]


def calculate_score(file, specie, threshold):
    [scores, cost] = parse_file(file)
    top_score = None
    top_specie = None
    correct_count = 0
    unknown_count = 0

    for fasta in scores.keys():
        if top_score is None or top_score < scores[fasta]:
            top_score = scores[fasta]
            top_specie = fasta

    if top_score < threshold:
        unknown_count += 1
        print("Unknown", file, "Top score", top_score)
    elif top_specie == specie:
        correct_count += 1
    else:
        print("Incorrect", file, "Expected", specie, "Actual", top_specie)

    return [correct_count, unknown_count, 1, 1, cost]


def graph(correct, unknown, wrong, threshold):
    fig, ax = plt.subplots()
    # x_values = list(correct.keys())
    # x_values.sort()
    print(correct)
    print(unknown)
    print(wrong)
    x_values = ["medic_500", "medic_1000", "medic_2000", "500", "1000", "2000"]
    indices = range(len(x_values))
    correct_values = []
    wrong_values = []
    unknown_values = []
    wrong_bottom = []

    for x_value in x_values:
        correct_values.append(correct[x_value])
        unknown_values.append(unknown[x_value])
        wrong_bottom.append(correct[x_value] + unknown[x_value])
        wrong_values.append(wrong[x_value])

    x_values = ["m500", "m1000", "m2000", "500", "1000", "2000"]
    p0 = plt.bar(indices, correct_values)
    p1 = plt.bar(indices, unknown_values, bottom=correct_values)
    p2 = plt.bar(indices, wrong_values, bottom=wrong_bottom)
    plt.legend((p0[0], p1[0], p2[0]), ("Correct", "Unknown", "Wrong"))
    plt.ylabel("Percentage (%)")
    plt.title("Spectra Labels (Threshold={0:f}%)".format(threshold * 100))
    plt.xticks(indices, x_values)
    plot_name = "data_counts/accuracy_{0:f}.png".format(threshold)
    plt.show()
    print("Plot", plot_name)
    fig.savefig(plot_name)
    plt.close()


def check_data(datasets):
    base_500_results = {}
    base_1000_results = {}
    base_2000_results = {}

    files = []
    i = 0
    for root, dirs, fs in os.walk("data_counts"):
        parts = root.split(os.sep)
        folder = "/".join(parts[2:])
        if len(parts) <= 2 or parts[2] == "PXD003177":
            print(parts, len(fs))
            continue
        if "_" not in parts[1]:
            for f in fs:
                if not f.endswith("mzML"):
                    continue
                key = folder + "/" + f
                file = "/".join(parts[:2]) + "/" + key
                files.append(file)
                [scores, cost] = parse_file(file)
                if parts[1] == "500":
                    base_500_results[key] = scores
                elif parts[1] == "1000":
                    base_1000_results[key] = scores
                else:
                    base_2000_results[key] = scores
                i += 1

    one_offs = [
        ["PES_Unfractionated-1533935400961/22Feb2011-PES-unfract-N2-L2-02.mzML", "silacLys6Arg6Human"],
        ["PXD002098-Mann_SILAC_Human_Cell_Lines", "superSilacLys4Arg6Lys8Arg10Cattle"],
        ["PXD002098-Mann_SILAC_Human_Cell_Lines/20101220_Velos2_SaDe_SA_TMD8(2)_02.mzML", "silacLys6Arg6Cattle"],
        ["PXD001250-Mann_Mouse_Brain_Proteome/20130202_EXQ5_KiSh_SA_neuro_adultmicroglia_rep2_5.mzML",
         "normalZebrafish"],
        ["PXD001250-Mann_Mouse_Brain_Proteome/20120901_EXQ5_KiSh_SA_Collab_neuroJ_insol_11.mzML",
         "phosphorylationRedJunglefowl"],
        ["PXD001250-Mann_Mouse_Brain_Proteome/20130208_EXQ5_KiSh_SA_neuro_intermediate_oligodend_rep2_11.mzML",
         "silacLys6Arg6Zebrafish"],
        ["PXD002801-TMT10/m00369_MDP_liver_fusion_tmt10_Set10_F05.mzML", "tmt6Mouse"],
        ["PXD002801-TMT10/m00369_MDP_liver_fusion_tmt10_Set10_F05.mzML", "tmt6PhosphorylationMouse"],
        ["PXD002801-TMT10/m00368_MDP_liver_fusion_tmt10_Set10_F04.mzML", "tmt6PhosphorylationHuman"],
        ["PXD002801-TMT10/m00354_MDP_liver_fusion_tmt10_Set9_F08.mzML", "tmt6Human"],
        ["PXD002801-TMT10/f01564_MDP_liver_fusion_tmt10_Set2_F01.mzML", "tmt6Rat"],
        ["PXD002801-TMT10/f01564_MDP_liver_fusion_tmt10_Set2_F01.mzML", "tmt6PhosphorylationRat"],
        ["PXD002801-TMT10/f01596_MDP_liver_fusion_tmt10_Set3_F10.mzML", "tmt6PhosphorylationBoar"],
        ["PXD002801-TMT10/f01596_MDP_liver_fusion_tmt10_Set3_F10.mzML", "tmt6Boar"],
        ["PXD002801-TMT10/m00131_MDP_liver_fusion_tmt10_Set1_F02.mzML", "tmt6PhosphorylationBoar"],
        ["PXD002801-TMT10/m00131_MDP_liver_fusion_tmt10_Set1_F02.mzML", "tmt6Boar"],
        ["PXD002801-TMT10/m00145_MDP_liver_fusion_tmt10_Set6_F01.mzML", "tmt6PhosphorylationCattle"],
        ["PXD002801-TMT10/m00145_MDP_liver_fusion_tmt10_Set6_F01.mzML", "tmt6Cattle"],
        ["PXD002801-TMT10/m00148_MDP_liver_fusion_tmt10_Set6_F04.mzML", "tmt6PhosphorylationCattle"],
        ["PXD002801-TMT10/m00148_MDP_liver_fusion_tmt10_Set6_F04.mzML", "tmt6Cattle"],
        ["PXD002801-TMT10/m00148_MDP_liver_fusion_tmt10_Set6_F04.mzML", "tmt6Rat"],
        ["PXD002801-TMT10/m00148_MDP_liver_fusion_tmt10_Set6_F04.mzML", "tmt6PhosphorylationRat"],
        ["PXD002801-TMT10/m00350_MDP_liver_fusion_tmt10_Set9_F04.mzML", "tmt6PhosphorylationBoar"],
        ["PXD002801-TMT10/m00350_MDP_liver_fusion_tmt10_Set9_F04.mzML", "tmt6Boar"],
        ["PXD002801-TMT10/m00352_MDP_liver_fusion_tmt10_Set9_F06.mzML", "tmt6Mouse"],
        ["PXD002801-TMT10/m00352_MDP_liver_fusion_tmt10_Set9_F06.mzML", "tmt6PhosphorylationMouse"],
        ["PXD002801-TMT10/m00354_MDP_liver_fusion_tmt10_Set9_F08.mzML", "tmt6PhosphorylationHuman"],
        ["PXD002801-TMT10/m00357_MDP_liver_fusion_tmt10_Set9_F11.mzML", "tmt6Mouse"],
        ["PXD002801-TMT10/m00357_MDP_liver_fusion_tmt10_Set9_F11.mzML", "tmt6PhosphorylationMouse"],
        ["PXD002801-TMT10/m00368_MDP_liver_fusion_tmt10_Set10_F04.mzML", "tmt6Human"],
        ["PXD002801-TMT10/m00374_MDP_liver_fusion_tmt10_Set10_F10.mzML", "tmt6PhosphorylationCattle"],
        ["PXD002801-TMT10/m00374_MDP_liver_fusion_tmt10_Set10_F10.mzML", "tmt6Cattle"],
        ["PXD002801-TMT10/t00116_MDP_liver_fusion_tmt10_Set00_F02.mzML", "tmt6PhosphorylationBoar"],
        ["PXD002801-TMT10/t00116_MDP_liver_fusion_tmt10_Set00_F02.mzML", "tmt6Boar"],
        ["PXD002801-TMT10/t00121_MDP_liver_fusion_tmt10_Set00_F07.mzML", "tmt6PhosphorylationCattle"],
        ["PXD002801-TMT10/t00121_MDP_liver_fusion_tmt10_Set00_F07.mzML", "tmt6Cattle"],
        ["ALS_CSF_Biomarker_Study-1530309740015/TN_CSF_062617_09.mzML", "normalHuman"],
        ["ALS_CSF_Biomarker_Study-1530309740015/TN_CSF_062617_15.mzML", "phosphorylationHuman"],
        ["PXD005709/150130-15_0325-01-AKZ-F01_150203130716.mzML", "normalMouse"],
        ["PXD005709/150130-15_0325-01-AKZ-F02.mzML", "silacLys6Arg6Boar"],
        ["PXD005709/150130-15_0326-01-AKZ-F01.mzML", "normalMouse"],
        ["PXD005709/150130-15_0329-01-AKZ-F01.mzML", "normalMouse"],
        ["PXD005709/150130-15_0330-01-AKZ-F03.mzML", "phosphorylationCattle"],
        ["PXD005709/150220-15_0328-01-AKZ-F03.mzML", "superSilacLys4Arg6Lys8Arg10Mouse"],
        ["PXD005709/150220-15_0328-01-AKZ-F03.mzML", "silacLys8Arg10Mouse"],
        ["PXD009220/MS150505-g9.mzML", "itraq8PhosphorylationBoar"],
        ["PXD009220/MS150505-g9.mzML", "itraq8PhosphorylationRat"],
        ["PXD009220/MS150505-g9.mzML", "itraq8Boar"],
        ["PXD009220/MS150505-g9.mzML", "itraq8Rat"],
        ["PXD009220/MS150505-g6.mzML", "itraq8PhosphorylationCattle"],
        ["PXD009220/MS150505-g6.mzML", "itraq8Cattle"],
        ["PXD009220/MS150505-g5.mzML", "itraq8RedJunglefowl"],
        ["PXD009220/MS150505-g5.mzML", "itraq8PhosphorylationRedJunglefowl"],
        ["PXD009220/MS150505-g1.mzML", "itraq8PhosphorylationBoar"],
        ["PXD009220/MS150505-g1.mzML", "itraq8Boar"],
        ["PXD009220/MS-G8.mzML", "itraq8PhosphorylationCattle"],
        ["PXD009220/MS-G8.mzML", "itraq8Cattle"],
        ["PXD009220/MS-G7.mzML", "itraq8RedJunglefowl"],
        ["PXD009220/MS-G7.mzML", "itraq8PhosphorylationRedJunglefowl"],
        ["PXD009220/MS-G1.mzML", "itraq8PhosphorylationCattle"],
        ["PXD009220/MS-G1.mzML", "itraq8Cattle"],
        ]

    total_keys = len(base_500_results)
    key_count = 0
    keys = list(base_500_results.keys())
    keys.sort()
    for key in keys:
        for score in base_500_results[key].keys():
            found = False
            for [key1, score1] in one_offs:
                if key == key1 and score == score1:
                    #          print("Weird", key, score, 500, base_500_results[key][score], 1000, base_1000_results[key][score], )
                    found = True
            if not found:
                if score not in base_1000_results[key]:
                    print("Sad", 1000, key_count, total_keys, key, score)
                elif score not in base_2000_results[key]:
                    print("Sad", 2000, key_count, total_keys, key, score)
        key_count += 1

    print("Num base files", len(files))
    print("")
    count = 0
    confidence = 0
    five_confidence = 0
    total = 0
    for i in range(len(files)):
        file = files[i]
        for comp in ["ppm_", "medic_"]:
            parts = file.split('/')
            parts[1] = comp + parts[1]
            key = os.sep.join(parts[2:])
            f = os.sep.join(parts)
            count += 1
            [scores, cost] = parse_file(f)
            if parts[1].endswith("500"):
                base_results = base_500_results
            elif parts[1].endswith("1000"):
                base_results = base_1000_results
            else:
                base_results = base_2000_results

            for k in scores.keys():
                if k not in base_results[key] or base_results[key][k] <= scores[k]:
                    confidence += 1
                if k not in base_results[key] or base_results[key][k] * 0.95 <= scores[k]:
                    five_confidence += 1
                total += 1

    print("Confidence", confidence, "95% Confdience", five_confidence, "Total", total)
    print("Num compare files", count)





if __name__ == "__main__":
    main()

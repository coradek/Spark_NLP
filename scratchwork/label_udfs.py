def author_labels(s):
    labels = {'JaneAusten': 0,
              'CharlesDickens': 1,
              'JohnMuir': 2,
              'MarkTwain': 3}
    return labels[s]


def title_labels(s):
    labels = {'Emma': 0,
              'MansfieldPark': 2,
              'Persuasion': 3,
              'PrideAndPrejudice': 4,
              'SenseAndSensibility': 5,
              'AChristmasCarol': 6,
              'DavidCopperfield': 7,
              'GreatExpectations': 8,
              'OliverTwist': 9,
              'ATaleOfTwoCities': 10,
              'MyFirstSummerInTheSierra': 11,
              'Stickeen': 12,
              'TheStoryofMyBoyhoodAndYouth': 13,
              'TravelsInAlaska': 14,
              'TheYosemite': 15,
              'TheAdventuresOfHuckleberryFinn': 16,
              'AConnecticutYankeeInKingArthursCourt': 17,
              'TheInnocentsAbroad': 18,
              'RoughingIt': 19,
              'TheTragedyofPuddnheadWilson': 20}
    return labels[s]

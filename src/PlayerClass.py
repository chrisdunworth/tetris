class Player():
    number_of_players = 0
    def __init__(self, name):
        Player.number_of_players += 1
        self.name = name
        self.score = 0     # New player starts with no points and 100 health meter
        self.health = 100
    def add_points(self, points):
        self.score += points
    def subtract_health(self, damage):
        self.health -= damage

player1 = Player("Fred")
player2 = Player("Bob")
player1.add_points(25)
player2.add_points(10)
print(player1.name + " has " + str(player1.score) + " points")
print("There are " + str(Player.number_of_players) + " players")

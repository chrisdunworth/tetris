import pygame
import random

# Setup
pygame.init()

HIT_SCREEN_BOTTOM = pygame.USEREVENT + 1
HIT_PADDLE = pygame.USEREVENT + 2

screen = pygame.display.set_mode([800,600])
timer = pygame.time.Clock()
font = pygame.font.SysFont("Arial", 18)

keep_going = True
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
GREEN = (0,255, 0)

class Game():
    MAXIMUM_STOOGES = 3
    __HITS_NEEDED_TO_ADD_STOOGE = 3
    
    def __init__(self):
        self.reset()

    def reset(self):
        self.__score = 0
        self.__lives = 5
        self.__consecutive_hits = 0

    def lose_a_life(self):
        self.__lives -= 1
        self.__consecutive_hits = 0

    def score_points(self, points):
        self.__score += points

    def record_a_hit(self):
        self.__consecutive_hits += 1

    def get_score(self):
        return self.__score

    def get_lives(self):
        return self.__lives

    def is_over(self):
        return self.__lives <= 0

    def should_we_add_a_stooge(self):
        return (self.__consecutive_hits > 0) and (self.__consecutive_hits % Game.__HITS_NEEDED_TO_ADD_STOOGE == 0)


class Paddle(pygame.sprite.Sprite):
    def __init__(self, color, centerx, y):
        pygame.sprite.Sprite.__init__(self)
        self.image = pygame.Surface([120, 20])
        self.image.fill(color)
        self.rect = self.image.get_rect()
        self.rect.centerx = centerx
        self.rect.y = y

    def set_pos(self, x):
        self.rect.centerx = x


class Stooge(pygame.sprite.Sprite):
    def __init__(self, speedx, speedy, x, y, image):
        pygame.sprite.Sprite.__init__(self)
        self.speedx = speedx
        self.speedy = speedy
        self.image = image
        self.rect = self.image.get_rect()
        self.rect.x = x
        self.rect.y = y

    def update(self, paddle):
        self.rect.x += self.speedx
        self.rect.y += self.speedy
        # hit left/right of screen?
        if self.rect.left <= 0 or self.rect.right >= screen.get_width():
            self.speedx = -self.speedx
        # hit top of screen?
        if self.rect.top <= 0:
            self.speedy = -self.speedy
        # hit bottom of screen?
        if self.rect.bottom >= screen.get_height():
            self.speedy = -self.speedy
            hit_screen_bottom( self )
        # hit paddle?
        if self.rect.bottom >= paddle.rect.y and self.rect.bottom < paddle.rect.bottom \
           and self.rect.centerx >= paddle.rect.left and self.rect.centerx <= paddle.rect.right \
           and self.speedy > 0:
            self.speedy = -self.speedy
            hit_paddle( self )

def spawn_stooge():
    image = pygame.image.load("/home/ctd/personal/python/stooge.png")
    return Stooge(random.choice([-3, 3]), 3, random.randint(0, screen.get_width() - image.get_width()), 0, image)

def hit_screen_bottom(stooge):
    event = pygame.event.Event(HIT_SCREEN_BOTTOM)
    event.stooge = stooge
    pygame.event.post( event )

def hit_paddle(stooge):
    event = pygame.event.Event(HIT_PADDLE)
    event.stooge = stooge
    pygame.event.post( event )

def render_text(text_string, color = WHITE):
    text = font.render(text_string, True, color)
    text_rect = text.get_rect()
    text_rect.centerx = screen.get_rect().centerx
    text_rect.y = 10
    screen.blit(text, text_rect)

# start the game
game = Game()

stooges = pygame.sprite.Group()
stooges.add( spawn_stooge() )
adding_a_stooge = False
last_hit_stooge = None

paddle = Paddle(WHITE, screen.get_rect().centerx, screen.get_rect().bottom - 50)
paddles = pygame.sprite.Group()
paddles.add( paddle )

# Game loop
while keep_going:
    # event loop
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            keep_going = False
        if event.type == HIT_SCREEN_BOTTOM:
            game.lose_a_life()
            stooges.remove( event.stooge )
            if len(stooges) == 0 and not game.is_over():
                stooges.add( spawn_stooge() )
        if event.type == HIT_PADDLE:
            game.score_points( len(stooges) )  # point per stooge
            game.record_a_hit()
            last_hit_stooge = event.stooge
            if game.should_we_add_a_stooge() and len(stooges) < Game.MAXIMUM_STOOGES:
                adding_a_stooge = True
        if event.type == pygame.KEYDOWN:
            if game.is_over() and event.key == pygame.K_RETURN:
                game.reset()
                stooges.add( spawn_stooge() )
            elif event.key == pygame.K_q or event.key == pygame.K_Q:
                keep_going = False

    if adding_a_stooge:
        if last_hit_stooge.rect.bottom <= paddle.rect.top - 75:
            stooges.add( spawn_stooge() )
            adding_a_stooge = False

    # move paddle, move stooges, and detect hits
    paddle.set_pos( pygame.mouse.get_pos()[0] )
    stooges.update( paddle )

    # draw everything
    screen.fill(BLACK)
    stooges.draw( screen )
    paddles.draw( screen )
    if game.is_over():
        stooges.empty()
        render_text( "GAME OVER. FINAL SCORE: " + str(game.get_score()) + ". Press Enter to play again, Q to quit.", GREEN )
    else:
        render_text( "LIVES: " + str(game.get_lives()) + " SCORE: " + str(game.get_score()) )

    pygame.display.update()

    timer.tick(120)

pygame.quit()

import pygame
pygame.init()

screen = pygame.display.set_mode([800,1000])

BOARD_WIDTH = 16
BOARD_HEIGHT = 20
BLOCK_SIZE = 50
BLACK = (0,0,0)

DROP_EVENT = pygame.USEREVENT + 1

stooge_image = pygame.transform.smoothscale( \
    pygame.image.load("/home/ctd/personal/python/img/stooge.png"), (BLOCK_SIZE, BLOCK_SIZE) )

class Piece():
    def __init__(self, image, x, y, grid1, grid2, grid3, grid4):
        self.image = image
        self.x = x
        self.y = y
        self.grids = [grid1, grid2, grid3, grid4]
        self.grid_index = 0

    def grid(self):
        return self.grids[self.grid_index]

    def move_left(self):
        if self.x > 0:
            self.x -= 1

    def move_right(self):
        if self.x < BOARD_WIDTH - len(self.grid()[0]):
            self.x += 1

    def move_down(self):
        if self.y < BOARD_HEIGHT - len(self.grid()):
            self.y += 1

    def rotate(self):
        self.grid_index = (self.grid_index + 1) % 4

def render_piece( piece, surface ):
    for y in range( len(piece.grid()) ):
        for x in range( len(piece.grid()[y]) ):
            if piece.grid()[y][x] == 1:
                screenx = (piece.x + x) * BLOCK_SIZE
                screeny = (piece.y + y) * BLOCK_SIZE
                surface.blit( piece.image, (screenx, screeny) )

# make grids for "reverse Z" piece
#grid1 = [[0,1,1],[1,1,0]]
#grid2 = [[1,0],[1,1],[0,1]]
#piece = Piece( stooge_image, 0, 0, grid1, grid2, grid1, grid2 )

# make grids for "L" piece
grid1 = [[1,1,1],[1,0,0]]
grid2 = [[1,1],[0,1],[0,1]]
grid3 = [[0,0,1],[1,1,1]]
grid4 = [[1,0],[1,0],[1,1]]
piece = Piece( stooge_image, 0, 0, grid1, grid2, grid3, grid4 )

pygame.time.set_timer( DROP_EVENT, 1000 )

running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_LEFT:
                piece.move_left()
            elif event.key == pygame.K_RIGHT:
                piece.move_right()
            elif event.key == pygame.K_UP:
                piece.rotate()
        elif event.type == DROP_EVENT:
            piece.move_down()

    # draw
    screen.fill(BLACK)
    render_piece( piece, screen )
    pygame.display.update()

pygame.quit()








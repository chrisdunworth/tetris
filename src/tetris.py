import pygame
import random
pygame.init()

screen = pygame.display.set_mode([800,1000])

BOARD_WIDTH = 16
BOARD_HEIGHT = 20
BLOCK_SIZE = 50
BLACK = (0,0,0)

DROP_EVENT = pygame.USEREVENT + 1

# Grids for each type of piece
SQUARE_GRIDS    = [ [[-1,-1,-1,-1],[-1,2,2,-1],[-1,2,2,-1],[-1,-1,-1,-1]] ]

BAR_GRIDS       = [ [[-1,-1,-1,-1],[5,5,5,5],[-1,-1,-1,-1],[-1,-1,-1,-1]],
                    [[-1,-1,5,-1],[-1,-1,5,-1],[-1,-1,5,-1],[-1,-1,5,-1]] ]

Z_GRIDS         = [ [[-1,-1,-1,-1],[-1,3,3,-1],[-1,-1,3,3],[-1,-1,-1,-1]],
                    [[-1,-1,-1,3],[-1,-1,3,3],[-1,-1,3,-1],[-1,-1,-1,-1]] ]

REVERSE_Z_GRIDS = [ [[-1,-1,-1,-1],[-1,-1,0,0],[-1,0,0,-1],[-1,-1,-1,-1]],
                    [[-1,-1,0,-1],[-1,-1,0,0],[-1,-1,-1,0],[-1,-1,-1,-1]] ]

L_GRIDS         = [ [[-1,-1,-1,-1],[-1,1,1,1],[-1,1,-1,-1],[-1,-1,-1,-1]],
                    [[-1,-1,1,-1],[-1,-1,1,-1],[-1,-1,1,1],[-1,-1,-1,-1]],
                    [[-1,-1,-1,1],[-1,1,1,1],[-1,-1,-1,-1],[-1,-1,-1,-1]],
                    [[-1,1,1,-1],[-1,-1,1,-1],[-1,-1,1,-1],[-1,-1,-1,-1]] ]

REVERSE_L_GRIDS = [ [[-1,-1,-1,-1],[-1,4,4,4],[-1,-1,-1,4],[-1,-1,-1,-1]],
                    [[-1,-1,4,4],[-1,-1,4,-1],[-1,-1,4,-1],[-1,-1,-1,-1]],
                    [[-1,4,-1,-1],[-1,4,4,4],[-1,-1,-1,-1],[-1,-1,-1,-1]],
                    [[-1,-1,4,-1],[-1,-1,4,-1],[-1,4,4,-1],[-1,-1,-1,-1]] ]

T_GRIDS         = [ [[-1,-1,-1,-1],[-1,6,6,6],[-1,-1,6,-1],[-1,-1,-1,-1]],
                    [[-1,-1,6,-1],[-1,-1,6,6],[-1,-1,6,-1],[-1,-1,-1,-1]],
                    [[-1,-1,6,-1],[-1,6,6,6],[-1,-1,-1,-1],[-1,-1,-1,-1]],
                    [[-1,-1,6,-1],[-1,6,6,-1],[-1,-1,6,-1],[-1,-1,-1,-1]] ]

# All piece grids in one list - spawn randomly from this
ALL_GRIDS = [ REVERSE_Z_GRIDS, Z_GRIDS, L_GRIDS, REVERSE_L_GRIDS, SQUARE_GRIDS, BAR_GRIDS, T_GRIDS ]

def new_board_grid() :
    return [[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,9],
            [9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9]]

# All images in a list - index
def scale_image(image_file):
    return pygame.transform.smoothscale( pygame.image.load(image_file), (BLOCK_SIZE, BLOCK_SIZE) )

images = [ scale_image("/home/ctd/personal/python/img/stooge1.png"),
           scale_image("/home/ctd/personal/python/img/stooge2.png"),
           scale_image("/home/ctd/personal/python/img/stooge3.png"),
           scale_image("/home/ctd/personal/python/img/stooge4.png"),
           scale_image("/home/ctd/personal/python/img/stooge5.png"),
           scale_image("/home/ctd/personal/python/img/stooge6.png"),
           scale_image("/home/ctd/personal/python/img/stooge7.png")]

class Piece():
    def __init__(self, x, y, grids):
        self.x = x
        self.y = y
        self.grids = grids
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
        self.grid_index = (self.grid_index + 1) % len(self.grids)

    def rows(self):
        return len( self.grid() )

    def columns(self):
        return len( self.grid()[0] )

    def cell(self, row, col):
        return self.grid()[row][col]

    
class Board():
    def __init__(self, grid):
        self.grid = grid

    def place_piece(self, piece):
        for y in range( piece.rows() ):
            for x in range( piece.columns() ):
                cell = piece.cell(y, x)
                if cell >= 0:
                    row = y + piece.y + 1
                    col = x + piece.x + 1
                    self.grid[row][col] = cell

def render_piece( piece, surface ):
    for y in range( piece.rows() ):
        for x in range( piece.columns() ):
            if piece.cell(y, x) >= 0:
                screenx = (piece.x + x) * BLOCK_SIZE
                screeny = (piece.y + y) * BLOCK_SIZE
                surface.blit( images[ piece.cell(y, x) ], (screenx, screeny) )

def render_board( board, surface ):
    for y in range(1, BOARD_HEIGHT + 1):
        for x in range(1, BOARD_WIDTH + 1):
            if board.grid[y][x] >= 0:
                screenx = (x - 1) * BLOCK_SIZE
                screeny = (y - 1) * BLOCK_SIZE
                surface.blit( images[ board.grid[y][x] ], (screenx, screeny) )

def spawn_piece():
    grids = random.choice(ALL_GRIDS)
    spawn_x = (BOARD_WIDTH - len(grids[0][0])) // 2
    spawn_y = 0
    return Piece( spawn_x, spawn_y, grids )


pygame.time.set_timer( DROP_EVENT, 1000 )
piece = spawn_piece()  # make the first piece
board = Board( new_board_grid() )
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
            elif event.key == pygame.K_s:
                board.place_piece( piece )
                piece = spawn_piece()  # make a new piece when 's' is pressed (just for testing purposes)
        elif event.type == DROP_EVENT:
            piece.move_down()

    # draw
    screen.fill(BLACK)
    render_board( board, screen )
    render_piece( piece, screen )
    pygame.display.update()

pygame.quit()








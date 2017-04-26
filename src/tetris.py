import pygame
import random
pygame.init()

screen = pygame.display.set_mode([800,1000])

BOARD_WIDTH = 10
BOARD_HEIGHT = 20
BOARD_BORDER = 2
PIECE_SIZE = 4
BLOCK_PIXELS = 50
BLACK = (0,0,0)
BLUE  = (0,0,50)
IMAGE_FOLDER = "/home/ctd/personal/python/img/"

DROP_EVENT = pygame.USEREVENT + 1

# Grids for each type of piece
SQUARE_GRIDS    = [ [[-1,-1,-1,-1],
                     [-1, 2, 2,-1],
                     [-1, 2, 2,-1],
                     [-1,-1,-1,-1]] ]

BAR_GRIDS       = [ [[-1,-1,-1,-1],
                     [ 5, 5, 5, 5],
                     [-1,-1,-1,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 5,-1],
                     [-1,-1, 5,-1],
                     [-1,-1, 5,-1],
                     [-1,-1, 5,-1]] ]

Z_GRIDS         = [ [[-1,-1,-1,-1],
                     [-1, 3, 3,-1],
                     [-1,-1, 3, 3],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1,-1, 3],
                     [-1,-1, 3, 3],
                     [-1,-1, 3,-1],
                     [-1,-1,-1,-1]] ]

REVERSE_Z_GRIDS = [ [[-1,-1,-1,-1],
                     [-1,-1, 0, 0],
                     [-1, 0, 0,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 0,-1],
                     [-1,-1, 0, 0],
                     [-1,-1,-1, 0],
                     [-1,-1,-1,-1]] ]

L_GRIDS         = [ [[-1,-1,-1,-1],
                     [-1, 1, 1, 1],
                     [-1, 1,-1,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 1,-1],
                     [-1,-1, 1,-1],
                     [-1,-1, 1, 1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1,-1, 1],
                     [-1, 1, 1, 1],
                     [-1,-1,-1,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1, 1, 1,-1],
                     [-1,-1, 1,-1],
                     [-1,-1, 1,-1],
                     [-1,-1,-1,-1]] ]

REVERSE_L_GRIDS = [ [[-1,-1,-1,-1],
                     [-1, 4, 4, 4],
                     [-1,-1,-1, 4],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 4, 4],
                     [-1,-1, 4,-1],
                     [-1,-1, 4,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1, 4,-1,-1],
                     [-1, 4, 4, 4],
                     [-1,-1,-1,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 4,-1],
                     [-1,-1, 4,-1],
                     [-1, 4, 4,-1],
                     [-1,-1,-1,-1]] ]

T_GRIDS         = [ [[-1,-1,-1,-1],
                     [-1, 6, 6, 6],
                     [-1,-1, 6,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 6,-1],
                     [-1,-1, 6, 6],
                     [-1,-1, 6,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 6,-1],
                     [-1, 6, 6, 6],
                     [-1,-1,-1,-1],
                     [-1,-1,-1,-1]],
                    
                    [[-1,-1, 6,-1],
                     [-1, 6, 6,-1],
                     [-1,-1, 6,-1],
                     [-1,-1,-1,-1]] ]

# All piece grids in one list - spawn randomly from this
ALL_GRIDS = [ REVERSE_Z_GRIDS, Z_GRIDS, L_GRIDS, REVERSE_L_GRIDS, SQUARE_GRIDS, BAR_GRIDS, T_GRIDS ]

def new_board_grid():
    return [[9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
            [9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9],
            [9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
            [9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9]]

def new_board_row():
    return [9, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 9, 9]

# All images in a list - index
def scale_image(image_file):
    return pygame.transform.smoothscale( pygame.image.load(image_file), (BLOCK_PIXELS, BLOCK_PIXELS) )

images = [ scale_image(IMAGE_FOLDER + "stooge1.png"),
           scale_image(IMAGE_FOLDER + "stooge2.png"),
           scale_image(IMAGE_FOLDER + "stooge3.png"),
           scale_image(IMAGE_FOLDER + "stooge4.png"),
           scale_image(IMAGE_FOLDER + "stooge5.png"),
           scale_image(IMAGE_FOLDER + "stooge6.png"),
           scale_image(IMAGE_FOLDER + "stooge7.png")]

class Piece():
    def __init__(self, x, y, grids):
        self.x = x
        self.y = y
        self.grids = grids
        self.grid_index = 0
        self.grid = self.grids[self.grid_index]

    def move_left(self, board):
        self.x -= 1
        if self.is_colliding(board):
            self.x += 1

    def move_right(self, board):
        self.x += 1
        if self.is_colliding(board):
            self.x -= 1

    def move_down(self, board):
        self.y += 1
        if self.is_colliding(board):
            self.y -= 1
            return False
        else:
            return True

    def rotate(self, board):
        self.grid_index = (self.grid_index + 1) % len(self.grids)
        self.grid = self.grids[self.grid_index]
        if self.is_colliding(board):
            self.grid_index = (self.grid_index - 1) % len(self.grids)
            self.grid = self.grids[self.grid_index]

    def drop_in_place(self, board):
        while not self.is_colliding(board):
            self.y += 1
        self.y -= 1
        board.place_piece( self )

    def is_colliding(self, board):
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                cell = self.grid[y][x]
                board_cell = board.grid[self.y + y][self.x + x]
                if cell > -1 and board_cell > -1:
                    return True
        return False

    
class Board():
    def __init__(self, grid):
        self.grid = grid

    def place_piece(self, piece):
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                cell = piece.grid[y][x]
                if cell >= 0:
                    row = y + piece.y
                    col = x + piece.x
                    self.grid[row][col] = cell
                    
    def clear_completed_rows(self):
        for y in range(BOARD_BORDER, BOARD_BORDER + BOARD_HEIGHT):
            if self.is_row_complete(y):
                self.clear_row(y)

    def is_row_complete(self, y):
        for cell in self.grid[y]:
            if cell == -1:
                return False
        return True
        
    def clear_row(self, y):
        del self.grid[y]
        self.grid.insert( 2, new_board_row() )
                    
    def is_onscreen(x, y):
        return x >= BOARD_BORDER and x < BOARD_WIDTH + BOARD_BORDER and y >= BOARD_BORDER and y < BOARD_HEIGHT + BOARD_BORDER


def render_piece( piece, surface ):
    for y in range( PIECE_SIZE ):
        for x in range( PIECE_SIZE ):
            boardx = piece.x + x
            boardy = piece.y + y
            if piece.grid[y][x] >= 0 and Board.is_onscreen(boardx, boardy):
                screenx = (boardx - BOARD_BORDER) * BLOCK_PIXELS
                screeny = (boardy - BOARD_BORDER) * BLOCK_PIXELS
                surface.blit( images[ piece.grid[y][x] ], (screenx, screeny) )

def render_board( board, surface ):
    for y in range( BOARD_BORDER, BOARD_HEIGHT + BOARD_BORDER ):
        for x in range( BOARD_BORDER, BOARD_WIDTH + BOARD_BORDER ):
            if board.grid[y][x] >= 0:
                screenx = (x - BOARD_BORDER) * BLOCK_PIXELS
                screeny = (y - BOARD_BORDER) * BLOCK_PIXELS
                surface.blit( images[ board.grid[y][x] ], (screenx, screeny) )

def spawn_piece():
    grids = random.choice(ALL_GRIDS)
    spawn_x = (BOARD_WIDTH - PIECE_SIZE) // 2 + BOARD_BORDER
    spawn_y = 1
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
                piece.move_left(board)
            elif event.key == pygame.K_RIGHT:
                piece.move_right(board)
            elif event.key == pygame.K_UP:
                piece.rotate(board)
            elif event.key == pygame.K_SPACE:
                piece.drop_in_place( board )
                board.clear_completed_rows()
                piece = spawn_piece()
        elif event.type == DROP_EVENT:
            moved = piece.move_down(board)
            if not moved:
                board.place_piece( piece )
                board.clear_completed_rows()
                piece = spawn_piece()

    # draw
    screen.fill(BLACK)
    pygame.draw.rect( screen, BLUE, pygame.Rect((500,0),(300,1000)) )
    render_board( board, screen )
    render_piece( piece, screen )
    pygame.display.update()

pygame.quit()








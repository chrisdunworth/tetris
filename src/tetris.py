import pygame
import random
pygame.init()

BOARD_WIDTH = 10
BOARD_HEIGHT = 20
BOARD_BORDER = 2
PIECE_SIZE = 4
BLOCK_PIXELS = 50
BLACK = (0,0,0)
BLUE  = (0,0,50)
WHITE = (255,255,255)
IMAGE_FOLDER = "/home/ctd/personal/python/img/"

DROP_EVENT = pygame.USEREVENT + 1
TIMER_SPEEDS = [1000, 900, 800, 700, 600, 500, 400, 300, 200, 100]
MAX_LEVEL = len(TIMER_SPEEDS)

POINTS_PER_ROW_CLEARED = [0, 100, 200, 400, 800]
ROWS_TO_LEVEL_UP = 10

NEXT_PIECE_X = 550
NEXT_PIECE_Y = 50

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
        original_y = self.y
        while not self.is_colliding(board):
            self.y += 1
        self.y -= 1
        board.place_piece( self )
        return self.y - original_y  # how far the piece fell when placed - used for scoring

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
        rows_cleared = 0
        for y in range(BOARD_BORDER, BOARD_BORDER + BOARD_HEIGHT):
            if self.is_row_complete(y):
                self.clear_row(y)
                rows_cleared += 1
        return rows_cleared  # used for scoring

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

def render_next_piece( piece, surface ):
    text = font.render("- Next Piece -", True, WHITE)
    surface.blit( text, (550, 25) )
    for y in range( PIECE_SIZE ):
        for x in range( PIECE_SIZE ):
            if piece.grid[y][x] >= 0:
                screenx = NEXT_PIECE_X + (x * BLOCK_PIXELS)
                screeny = NEXT_PIECE_Y + (y * BLOCK_PIXELS)
                surface.blit( images[ piece.grid[y][x] ], (screenx, screeny) )
              

def render_board( board, surface ):
    for y in range( BOARD_BORDER, BOARD_HEIGHT + BOARD_BORDER ):
        for x in range( BOARD_BORDER, BOARD_WIDTH + BOARD_BORDER ):
            if board.grid[y][x] >= 0:
                screenx = (x - BOARD_BORDER) * BLOCK_PIXELS
                screeny = (y - BOARD_BORDER) * BLOCK_PIXELS
                surface.blit( images[ board.grid[y][x] ], (screenx, screeny) )

def render_score( score, font, surface ):
    text = font.render("Score: " + str(score), True, WHITE)
    surface.blit( text, (550, 250) )

def render_level( level, font, surface ):
    text = font.render("Level: " + str(level), True, WHITE)
    surface.blit( text, (550, 300) )

def render_rows_cleared( rows, font, surface ):
    text = font.render("Rows: " + str(rows), True, WHITE)
    surface.blit( text, (550, 350) )

def spawn_piece():
    grids = random.choice(ALL_GRIDS)
    spawn_x = (BOARD_WIDTH - PIECE_SIZE) // 2 + BOARD_BORDER
    spawn_y = 1
    return Piece( spawn_x, spawn_y, grids )


screen = pygame.display.set_mode([800,1000])
pygame.time.set_timer( DROP_EVENT, 1000 )
font = pygame.font.SysFont("arial", 20)

score = 0
level = 1
total_rows_cleared = 0

piece = spawn_piece()  # make the first piece
next_piece = spawn_piece()
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
                height_dropped = piece.drop_in_place( board )
                rows_cleared = board.clear_completed_rows()
                score += height_dropped + POINTS_PER_ROW_CLEARED[rows_cleared]
                total_rows_cleared += rows_cleared
                piece = next_piece
                next_piece = spawn_piece()
        elif event.type == DROP_EVENT:
            moved = piece.move_down(board)
            if not moved:
                board.place_piece( piece )
                rows_cleared = board.clear_completed_rows()
                total_rows_cleared += rows_cleared
                score += POINTS_PER_ROW_CLEARED[rows_cleared]
                piece = next_piece
                next_piece = spawn_piece()
                    

    # level-up?
    if (total_rows_cleared // ROWS_TO_LEVEL_UP) == level and level < MAX_LEVEL:
        level += 1
        pygame.time.set_timer( DROP_EVENT, 0 )
        pygame.time.set_timer( DROP_EVENT, TIMER_SPEEDS[level - 1] )

    # draw
    screen.fill(BLACK)
    pygame.draw.rect( screen, BLUE, pygame.Rect((500,0),(300,1000)) )
    render_board( board, screen )
    render_piece( piece, screen )
    render_score( score, font, screen )
    render_level( level, font, screen )
    render_rows_cleared( total_rows_cleared, font, screen )
    render_next_piece( next_piece, screen )
    pygame.display.update()

pygame.quit()








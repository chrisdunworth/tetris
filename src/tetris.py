import pygame
import random
pygame.init()

BOARD_WIDTH = 10
BOARD_HEIGHT = 20
BOARD_BORDER = 2
PIECE_SIZE = 4

BLOCK_PIXELS = 40
GAME_SCREEN_WIDTH = BOARD_WIDTH * BLOCK_PIXELS
GAME_SCREEN_HEIGHT = BOARD_HEIGHT * BLOCK_PIXELS
STAT_SCREEN_WIDTH = GAME_SCREEN_WIDTH // 5 * 3

BLACK = (0,0,0)
BLUE  = (0,0,50)
WHITE = (255,255,255)
IMAGE_FOLDER = "/home/ctd/personal/python/img/"

DROP_EVENT = pygame.USEREVENT + 1
TIMER_SPEEDS = [1000, 900, 800, 700, 600, 500, 400, 300, 200, 100]
MAX_LEVEL = len(TIMER_SPEEDS)

POINTS_PER_ROW_CLEARED = [0, 100, 200, 400, 800]
ROWS_TO_LEVEL_UP = 10

NEXT_PIECE_X = GAME_SCREEN_WIDTH + BLOCK_PIXELS
NEXT_PIECE_Y = BLOCK_PIXELS

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


    

# All images in a list - index
def scale_image(image_file):
    return pygame.transform.smoothscale( pygame.image.load(image_file), (BLOCK_PIXELS, BLOCK_PIXELS) )

IMAGES = [
         [scale_image(IMAGE_FOLDER + "stooge1.png"),
          scale_image(IMAGE_FOLDER + "stooge2.png"),
          scale_image(IMAGE_FOLDER + "stooge3.png"),
          scale_image(IMAGE_FOLDER + "stooge4.png"),
          scale_image(IMAGE_FOLDER + "stooge5.png"),
          scale_image(IMAGE_FOLDER + "stooge6.png"),
          scale_image(IMAGE_FOLDER + "stooge7.png")],
             
         [scale_image(IMAGE_FOLDER + "programmer1.png"),
          scale_image(IMAGE_FOLDER + "programmer2.png"),
          scale_image(IMAGE_FOLDER + "programmer3.png"),
          scale_image(IMAGE_FOLDER + "programmer4.png"),
          scale_image(IMAGE_FOLDER + "programmer5.png"),
          scale_image(IMAGE_FOLDER + "programmer6.png"),
          scale_image(IMAGE_FOLDER + "programmer7.png")]
         ]


class Piece():
    def __init__(self, x, y, grids, board):
        self.x = x
        self.y = y
        self.grids = grids
        self.grid_index = 0
        self.grid = self.grids[self.grid_index]
        self.board = board

    def move_left(self):
        self.x -= 1
        if self.is_colliding():
            self.x += 1

    def move_right(self):
        self.x += 1
        if self.is_colliding():
            self.x -= 1

    def move_down(self):
        self.y += 1
        if self.is_colliding():
            self.y -= 1
            return False
        else:
            return True

    def rotate(self):
        self.grid_index = (self.grid_index + 1) % len(self.grids)
        self.grid = self.grids[self.grid_index]
        if self.is_colliding():
            self.grid_index = (self.grid_index - 1) % len(self.grids)
            self.grid = self.grids[self.grid_index]

    def drop_in_place(self):
        original_y = self.y
        while not self.is_colliding():
            self.y += 1
        self.y -= 1
        self.board.place_piece( self )
        return self.y - original_y  # how far the piece fell when placed - used for scoring

    def is_colliding(self):
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                cell = self.grid[y][x]
                board_cell = self.board.grid[self.y + y][self.x + x]
                if cell > -1 and board_cell > -1:
                    return True
        return False

    
class Board():
    def __init__(self, width, height, border):
        self.width = width
        self.height = height
        self.border = border
        self.initialize_game_grid()
    
    def initialize_game_grid(self):
        self.grid = []
        for i in range(self.border):
            self.grid.append( self.new_border_row() )
        for i in range(self.height):
            self.grid.append( self.new_empty_row() )
        for i in range(self.border):
            self.grid.append( self.new_border_row() )

    def new_border_row(self):
        row = []
        for i in range(self.border * 2 + self.width):
            row.append(9)
        return row
            
    def new_empty_row(self):
        row = []
        for i in range(self.border):
            row.append(9)
        for i in range(self.width):
            row.append(-1)
        for i in range(self.border):
            row.append(9)
        return row

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
        for y in range(self.border, self.border + self.height):
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
        self.grid.insert( 2, self.new_empty_row() )
                    
    def is_onscreen(self, x, y):
        return x >= self.border and x < self.width + self.border and y >= self.border and y < self.height + self.border


class Game():
    
    PLAYING = 1
    GAME_OVER = 2
    
    def __init__(self):
        self.score = 0
        self.level = 1
        self.total_rows_cleared = 0
        self.board = Board( BOARD_WIDTH, BOARD_HEIGHT, BOARD_BORDER )
        self.piece = self.spawn_piece()
        self.next_piece = self.spawn_piece()
        self.state = Game.PLAYING
        self.image_set = 0
        self.set_drop_timer()

    def play_next_piece(self):
        self.piece = self.next_piece
        self.next_piece = self.spawn_piece()

    def score_points(self, points):
        self.score += points

    def rows_cleared(self, rows):
        self.total_rows_cleared += rows
        self.score += POINTS_PER_ROW_CLEARED[rows]

    def spawn_piece(self):
        grids = random.choice(ALL_GRIDS)
        spawn_x = (BOARD_WIDTH - PIECE_SIZE) // 2 + BOARD_BORDER
        spawn_y = 1
        return Piece( spawn_x, spawn_y, grids, self.board )

    def place_piece(self):
        height_dropped = self.piece.drop_in_place()
        self.score_points( height_dropped )
        rows_cleared = self.board.clear_completed_rows()
        self.rows_cleared( rows_cleared )
        self.level_up_if_necessary()
        self.play_next_piece()
        if ( self.piece_is_colliding() ):
            self.state = Game.GAME_OVER

    def move_piece_left(self):
        self.piece.move_left()

    def move_piece_right(self):
        self.piece.move_right()

    def rotate_piece(self):
        self.piece.rotate()

    def move_piece_down(self):
        return self.piece.move_down()

    def piece_is_colliding(self):
        return self.piece.is_colliding()

    def next_image_set(self):
        self.image_set = (self.image_set + 1) % len(IMAGES)

    def level_up_if_necessary(self):
        if (self.total_rows_cleared // ROWS_TO_LEVEL_UP) == self.level and self.level < MAX_LEVEL:
            self.level += 1
            self.set_drop_timer()

    def set_drop_timer(self):
        pygame.time.set_timer( DROP_EVENT, 0 )
        pygame.time.set_timer( DROP_EVENT, TIMER_SPEEDS[self.level - 1] )
        

class Renderer():
    
    def __init__(self, surface, font, bigfont):
        self.surface = surface
        self.font = font
        self.bigfont = bigfont

    def render_all(self, game):
        screen.fill(BLACK)
        pygame.draw.rect( screen, BLUE, pygame.Rect((GAME_SCREEN_WIDTH,0),(STAT_SCREEN_WIDTH,GAME_SCREEN_HEIGHT)) )
        self.render_board( game )
        self.render_piece( game )
        self.render_score( game )
        self.render_level( game )
        self.render_rows_cleared( game )
        self.render_next_piece( game )
        if ( game.state == Game.GAME_OVER ):
            self.render_game_over()
        
    def render_piece(self, game ):
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                boardx = game.piece.x + x
                boardy = game.piece.y + y
                if game.piece.grid[y][x] >= 0 and game.board.is_onscreen(boardx, boardy):
                    screenx = (boardx - game.board.border) * BLOCK_PIXELS
                    screeny = (boardy - game.board.border) * BLOCK_PIXELS
                    self.surface.blit( IMAGES[game.image_set][ game.piece.grid[y][x] ], (screenx, screeny) )

    def render_next_piece(self, game):
        text = self.font.render("- Next Piece -", True, WHITE)
        self.surface.blit( text, (GAME_SCREEN_WIDTH + BLOCK_PIXELS, BLOCK_PIXELS//2 ) )
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                if game.next_piece.grid[y][x] >= 0:
                    screenx = NEXT_PIECE_X + (x * BLOCK_PIXELS)
                    screeny = NEXT_PIECE_Y + (y * BLOCK_PIXELS)
                    self.surface.blit( IMAGES[game.image_set][ game.next_piece.grid[y][x] ], (screenx, screeny) )
                  

    def render_board(self, game):
        for y in range( game.board.border, game.board.height + game.board.border ):
            for x in range( game.board.border, game.board.width + game.board.border ):
                if game.board.grid[y][x] >= 0:
                    screenx = (x - game.board.border) * BLOCK_PIXELS
                    screeny = (y - game.board.border) * BLOCK_PIXELS
                    self.surface.blit( IMAGES[game.image_set][ game.board.grid[y][x] ], (screenx, screeny) )

    def render_score(self, game):
        text = self.font.render("Score: " + str(game.score), True, WHITE)
        self.surface.blit( text, (GAME_SCREEN_WIDTH + BLOCK_PIXELS, BLOCK_PIXELS * 5) )

    def render_level(self, game):
        text = self.font.render("Level: " + str(game.level), True, WHITE)
        self.surface.blit( text, (GAME_SCREEN_WIDTH + BLOCK_PIXELS, BLOCK_PIXELS * 6) )

    def render_rows_cleared(self, game):
        text = self.font.render("Rows: " + str(game.total_rows_cleared), True, WHITE)
        self.surface.blit( text, (GAME_SCREEN_WIDTH + BLOCK_PIXELS, BLOCK_PIXELS * 7) )

    def render_game_over(self):
        text = self.bigfont.render("-: GAME OVER :-", True, WHITE)
        self.surface.blit( text, (GAME_SCREEN_WIDTH//2, GAME_SCREEN_HEIGHT // 5 * 2) )

  

screen = pygame.display.set_mode([GAME_SCREEN_WIDTH + STAT_SCREEN_WIDTH, GAME_SCREEN_HEIGHT])
font = pygame.font.SysFont("arial", BLOCK_PIXELS // 5 * 3)
bigfont = pygame.font.SysFont("arial", BLOCK_PIXELS)
renderer = Renderer(screen, font, bigfont)

game = Game()
running = True

while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_ESCAPE:
                running = False
            if ( game.state == Game.PLAYING ):
                if event.key == pygame.K_LEFT:
                    game.move_piece_left()
                elif event.key == pygame.K_RIGHT:
                    game.move_piece_right()
                elif event.key == pygame.K_UP:
                    game.rotate_piece()
                elif event.key == pygame.K_SPACE:
                    game.place_piece()
                elif event.key == pygame.K_e:
                    game.next_image_set()
            elif ( game.state == Game.GAME_OVER ):
                if event.key == pygame.K_p:
                    # new game
                    game = Game()
        elif event.type == DROP_EVENT:
            if ( game.state == Game.PLAYING ):
                moved = game.move_piece_down()
                if not moved:
                    game.place_piece()

    # draw
    renderer.render_all(game)
    pygame.display.update()

pygame.quit()








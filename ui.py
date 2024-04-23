import time
import threading
import multiprocessing as mp

import dearpygui.dearpygui as dpg
import tkinter as tk
from tkinter import filedialog

from compute import process_data, write_to_excel

##### GLOBAL VARIABLES #####

input_file = ""
output_file = ""
progress = 0.0
error_message = ""

##### GET EXISTING FILE PATH #####

def get_filename_with_tk(return_dict):
  root = tk.Tk()
  root.withdraw()
  file_path = filedialog.askopenfilename(filetypes=[("Excel files", ".xlsx .xls")])
  root.destroy()
  return_dict['val'] = file_path

def get_filename():
  return_dict = mp.Manager().dict()
  proc = mp.Process(target=get_filename_with_tk, args=(return_dict, ))
  proc.start()
  proc.join()
  return return_dict['val']


##### GET NEW FILE PATH #####

def get_new_filename_with_tk(return_dict):
  root = tk.Tk()
  root.withdraw()
  file_path = filedialog.asksaveasfilename(filetypes=[("Excel files", ".xlsx .xls")])
  root.destroy()
  return_dict['val'] = file_path

def get_new_filename():
  return_dict = mp.Manager().dict()
  proc = mp.Process(target=get_new_filename_with_tk, args=(return_dict, ))
  proc.start()
  proc.join()
  return return_dict['val']


##### APPLICATION SPECIFIC #####

def get_input_file():
  global input_file
  input_file = get_filename()
  dpg.set_value("input_file_text", f"Input File: {input_file}")

def get_output_file():
  global output_file
  output_file = get_new_filename()
  dpg.set_value("output_file_text", f"Output File: {output_file}")


##### PROCESS DATA #####

def process_data_with_error_handling(input_file, output_file, write_to_excel, progress_callback, error_callback):
  try:
    process_data(input_file, output_file, write_to_excel, progress_callback)
  except Exception as e:
    error_callback(e)

def process_data_with_ui():
  thread = threading.Thread(target=process_data_with_error_handling, args=(input_file, output_file, write_to_excel, lambda x: globals().update(progress = x), lambda x: globals().update(error_message = x)))
  thread.start()



##### UPDATE UI #####
def update_progress_bar(progress_bar):
  global progress
  while True:
    time.sleep(0.1)
    dpg.set_value(progress_bar, progress)
    dpg.configure_item(progress_bar, overlay=f"{progress:.0%}")

def check_error():
  global error_message
  while True:
    time.sleep(0.1)
    if error_message != "":
      with dpg.window(label="Error", modal=True):
        dpg.add_text("An error occurred during processing. Most likely your input file is malformed. Please check the input file and try again.")
        dpg.add_text("The error message is: ")
        dpg.add_text(error_message)
      error_message = ""

      dpg.configure_item("input_button", enabled=True)
      dpg.configure_item("output_button", enabled=True)
      dpg.configure_item("process_button", enabled=True)

def input_button_pressed():
  get_input_file()

def output_button_pressed():
  get_output_file()

def process_button_pressed():
  if input_file == "":
    with dpg.window(label="Error", modal=True):
      dpg.add_text("Please select an input file")
    return
  elif output_file == "":
    with dpg.window(label="Error", modal=True):
      dpg.add_text("Please select an output file")
    return

  dpg.configure_item("input_button", enabled=False)
  dpg.configure_item("output_button", enabled=False)
  dpg.configure_item("process_button", enabled=False)

  process_data_with_ui()




#### Goofy MP Workaround for pyinstaller ####

def mp_enable_freeze():
  mp.freeze_support()


##### MAIN #####

def main():
  dpg.create_context()
  dpg.create_viewport(height=300)
  dpg.setup_dearpygui()


  with dpg.window(tag="primary"):
    with dpg.table(header_row=False):
      for _ in range(3):
        dpg.add_table_column()

      with dpg.table_row():
        with dpg.table_cell():
          dpg.add_text("Input File:", tag="input_file_text")
          dpg.add_button(label="Select Input File", tag="input_button", callback=input_button_pressed)
        with dpg.table_cell():
          dpg.add_text("Output File:", tag="output_file_text")
          dpg.add_button(label="Select Output File", tag="output_button",callback=output_button_pressed)
        with dpg.table_cell():
          dpg.add_text("Process Data:")
          dpg.add_button(label="Process Data", tag="process_button", callback=process_button_pressed)
          progress_bar = dpg.add_progress_bar(label="Progress", default_value=0.0, overlay="0%")

  
  threading.Thread(target=update_progress_bar, args=(progress_bar, ), daemon=True).start()
  threading.Thread(target=check_error, daemon=True).start()

  dpg.set_primary_window("primary", True)

  dpg.show_viewport()
  dpg.start_dearpygui()
  dpg.destroy_context()

if __name__ == "__main__":
  mp_enable_freeze()
  main()
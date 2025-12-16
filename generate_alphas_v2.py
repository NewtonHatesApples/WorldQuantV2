import tkinter as tk
from tkinter import simpledialog, messagebox, ttk
import random
import re
import csv
from api import get_operators, get_datafields  # Assuming api.py is in the same directory

# Global variables
session = None
operators_dict = None
variables = {}  # To store variable definitions

class AlphaTemplate:
    def __init__(self, template_str, variables):
        """
        Initialize AlphaTemplate.
        :param template_str: The template string with placeholders like <var_name/>
        :param variables: Dict of variable definitions
        """
        self.template = template_str
        self.variables = variables

class ToolTip:
    def __init__(self, widget):
        self.widget = widget
        self.tipwindow = None

    def showtip(self, text, x, y):
        if self.tipwindow or not text:
            return
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=text, justify=tk.LEFT,
                         background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                         font=("tahoma", "8", "normal"))
        label.pack(ipadx=1)

    def hidetip(self):
        if self.tipwindow:
            self.tipwindow.destroy()
        self.tipwindow = None

def add_variable():
    # Dialog for variable name and type
    dialog = tk.Toplevel(root)
    dialog.title("Add Variable")

    tk.Label(dialog, text="Variable Name:").grid(row=0, column=0)
    name_entry = tk.Entry(dialog)
    name_entry.grid(row=0, column=1)

    tk.Label(dialog, text="Type:").grid(row=1, column=0)
    type_var = tk.StringVar()
    type_menu = ttk.Combobox(dialog, textvariable=type_var, values=["operators", "datafields", "number"])
    type_menu.grid(row=1, column=1)

    def ok():
        name = name_entry.get().strip()
        var_type = type_var.get()
        if not name or not var_type:
            messagebox.showerror("Error", "Name and type are required.")
            return

        if name in variables:
            messagebox.showerror("Error", "Variable name already exists.")
            return

        if var_type == "operators":
            select_operators(name)
        elif var_type == "datafields":
            select_datafields(name)
        elif var_type == "number":
            select_number(name)

        dialog.destroy()

    tk.Button(dialog, text="OK", command=ok).grid(row=2, column=0, columnspan=2)

def select_operators(name):
    global operators_dict
    if operators_dict is None:
        operators_dict = get_operators(session)
        if operators_dict is None:
            messagebox.showerror("Error", "Failed to fetch operators.")
            return

    # Get unique categories
    categories = set(op["category"] for op in operators_dict.values())
    categories = ["All"] + sorted(list(categories))

    # Dialog for selecting operators
    op_dialog = tk.Toplevel(root)
    op_dialog.title("Select Operators")

    # Filter entry
    tk.Label(op_dialog, text="Filter by name:").grid(row=0, column=0)
    filter_entry = tk.Entry(op_dialog)
    filter_entry.grid(row=0, column=1)

    # Category menu
    tk.Label(op_dialog, text="Category:").grid(row=1, column=0)
    cat_var = tk.StringVar(value="All")
    cat_menu = ttk.Combobox(op_dialog, textvariable=cat_var, values=categories)
    cat_menu.grid(row=1, column=1)

    # Listbox
    listbox = tk.Listbox(op_dialog, selectmode=tk.MULTIPLE, height=20, width=50)
    listbox.grid(row=2, column=0, columnspan=2)

    def update_list():
        listbox.delete(0, tk.END)
        filter_text = filter_entry.get().lower()
        selected_cat = cat_var.get()
        for op_name in sorted(operators_dict.keys()):
            op = operators_dict[op_name]
            if filter_text in op_name.lower():
                if selected_cat == "All" or op["category"] == selected_cat:
                    listbox.insert(tk.END, op_name)

    update_list()

    filter_entry.bind("<KeyRelease>", lambda e: update_list())
    cat_menu.bind("<<ComboboxSelected>>", lambda e: update_list())

    # Tooltip setup
    tool_tip = ToolTip(listbox)

    def on_motion(event):
        tool_tip.hidetip()
        idx = listbox.nearest(event.y)
        if idx >= 0:
            op_name = listbox.get(idx)
            op = operators_dict[op_name]
            text = f"Definition: {op['definition']}\nDescription: {op['description']}"
            root_x = listbox.winfo_rootx()
            root_y = listbox.winfo_rooty()
            bbox = listbox.bbox(idx)
            if bbox:
                rel_x, rel_y, w, h = bbox
                abs_x = root_x + rel_x + w + 10  # to the right
                abs_y = root_y + rel_y
                tool_tip.showtip(text, abs_x, abs_y)

    listbox.bind("<Motion>", on_motion)
    listbox.bind("<Leave>", lambda e: tool_tip.hidetip())

    def select():
        selected = [listbox.get(i) for i in listbox.curselection()]
        if not selected:
            messagebox.showerror("Error", "Select at least one operator.")
            return
        variables[name] = {"type": "operators", "values": selected}
        # Insert placeholder into template entry
        template_entry.insert(tk.INSERT, f"<{name}/>")
        op_dialog.destroy()

    tk.Button(op_dialog, text="Select", command=select).grid(row=3, column=0, columnspan=2)

def select_datafields(name):
    while True:
        dataset_id = simpledialog.askstring("Dataset ID", "Enter Dataset ID:")
        if dataset_id is None:
            return  # Cancelled
        region = simpledialog.askstring("Region", "Enter Region (e.g., USA):") or "USA"
        dataType = simpledialog.askstring("Data Type", "Enter Data Type (e.g., VECTOR, MATRIX):") or "VECTOR"
        universe = simpledialog.askstring("Universe", "Enter Universe (e.g., TOP3000):") or "TOP3000"
        delay_str = simpledialog.askstring("Delay", "Enter Delay (0 or 1):") or "1"
        try:
            delay = int(delay_str)
            if delay not in [0, 1]:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Invalid delay. Must be 0 or 1.")
            continue

        # Fetch datafields
        fields = get_datafields(
            session, dataset_id, region=region, dataType=dataType, universe=universe, delay=delay,
            instrumentType="EQUITY", limit=50  # Full fetch handled in function
        )
        if fields is None or len(fields) == 0:
            messagebox.showerror("Error", "Dataset does not exist or no fields found. Try again.")
            continue
        variables[name] = {"type": "datafields", "values": list(fields.keys())}
        template_entry.insert(tk.INSERT, f"<{name}/>")
        break

def select_number(name):
    values_str = simpledialog.askstring("Number Values", "Enter comma-separated numbers (e.g., 5,10,20):")
    if values_str is None:
        return
    try:
        values = [float(v.strip()) for v in values_str.split(',') if v.strip()]
        if not values:
            raise ValueError
        variables[name] = {"type": "number", "values": values}
        template_entry.insert(tk.INSERT, f"<{name}/>")
    except ValueError:
        messagebox.showerror("Error", "Invalid numbers.")

def generate_alphas(alpha_template, amount, filename):
    alphas = []
    unique_vars = set(re.findall(r"<(\w+)/>", alpha_template.template))

    for _ in range(amount):
        replacements = {}
        for var in unique_vars:
            var_info = alpha_template.variables.get(var)
            if var_info is None:
                raise ValueError(f"Variable {var} not defined.")
            if var_info["type"] in ["operators", "datafields", "number"]:
                repl = random.choice(var_info["values"])
                if isinstance(repl, float) and repl.is_integer():
                    repl = int(repl)
                replacements[var] = str(repl)

        alpha = alpha_template.template
        for var, repl in replacements.items():
            alpha = alpha.replace(f"<{var}/>", repl).replace("\"", "")
        alphas.append(alpha)

    with open(filename, "a") as f:
        for alpha in alphas:
            f.write(alpha + "\n")

    print(f"Generated {amount} alphas and saved to {filename}")

def generate_alphas_save_to_csv(auth_session, filename, amount=2500):
    global session, variables, operators_dict
    session = auth_session
    variables = {}
    operators_dict = None

    global root, template_entry
    root = tk.Tk()
    root.title("Alpha Template Builder")

    template_entry = tk.Text(root, height=10, width=80)
    template_entry.pack()

    add_btn = tk.Button(root, text="Add Variable", command=add_variable)
    add_btn.pack()

    def finish():
        template_str = template_entry.get("1.0", tk.END).strip()
        if not template_str:
            messagebox.showerror("Error", "Template is empty.")
            return
        alpha_template = AlphaTemplate(template_str, variables)
        root.destroy()
        generate_alphas(alpha_template, amount, filename)

    finish_btn = tk.Button(root, text="Finish!", command=finish)
    finish_btn.pack()

    root.mainloop()


if __name__ == "__main__":
    print("Finally done this shit")

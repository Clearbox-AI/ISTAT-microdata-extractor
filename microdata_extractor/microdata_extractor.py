from typing import List, Dict, Tuple, Any, Literal, Optional, Sequence, Union
import copy
import glob 

import os
import warnings

import polars as pl
import polars.selectors as cs
import pandas as pd

from bs4 import BeautifulSoup
from functools import reduce
import operator

class ISTATMicrodataExtractor:
    def __init__(
            self, 
            df_name: Literal["AVQ","HBS"],
            year=2023, 
            get_polars=False
        ):        
        """
        Initialize ISTAT Microdata Extractor.
        Available datasets:
        - AVQ: "Indagine sugli Aspetti della Vita Quotidiana (AVQ) delle famiglie italiane"
        - HBS: "Indagine sulle spese delle famiglie italiane"
        .
        """
        self._OPS = {
            "==":  lambda c, v: c == v,
            "!=":  lambda c, v: c != v,
            ">":   lambda c, v: c >  v,
            ">=":  lambda c, v: c >= v,
            "<":   lambda c, v: c <  v,
            "<=":  lambda c, v: c <= v,
            "in":       lambda c, v: c.is_in(list(v)),
            "not in":   lambda c, v: ~c.is_in(list(v)),
        }
        self.df_name = df_name
        self.year = year
        self.get_polars = get_polars

    def load_data(
            self,
            path_to_main_folder,
        ) -> None:
        """
        Load the ISTAT microdata from the specified folder.
        Parameters
        ----------
        path_to_main_folder : str
            Path to the main folder containing the METADATA and MICRODATA folders.
        Raises
        ------
        FileNotFoundError
            If the specified path does not exist or if the required files are not found.
        """
        # Check if paths exist
        self.path_to_main_folder = path_to_main_folder
        if not os.path.exists(os.path.join(path_to_main_folder,"MICRODATI/")):
            raise FileNotFoundError(f"Directory not found: {path_to_main_folder}/MICRODATI/")
        list_of_csv_files = glob.glob(os.path.join(path_to_main_folder,"MICRODATI/*.csv"))
        if not list_of_csv_files or len(list_of_csv_files) > 1:
            list_of_txt_files = glob.glob(os.path.join(path_to_main_folder,"MICRODATI/*.txt"))
            if not list_of_txt_files or len(list_of_txt_files) > 1:
                raise FileNotFoundError(f"{path_to_main_folder}/MICRODATI should contian exactly one .txt file and/or one .csv file, but {len(list_of_txt_files)} .txt files and {len(list_of_csv_files)} were found.")
            path_to_df = list_of_txt_files[0]
            if not os.path.exists(path_to_df):
                raise FileNotFoundError(f"File not found: {path_to_df}")
            # Load the Microdata DataFrame from txt
            self.df = pl.read_csv(
                path_to_df,
                separator="\t",
                null_values=[" " * n for n in range(1, 13)],
                encoding="utf8",
                infer_schema_length=None, 
                quote_char=None,
                truncate_ragged_lines=True
            )        
        else:
            # Load the Microdata DataFrame from csv
            path_to_df = list_of_csv_files[0]
            self.df = pl.read_csv(
                path_to_df,
                null_values=[" " * n for n in range(1, 13)],
                infer_schema_length=None, 
                quote_char=None,
                truncate_ragged_lines=True)

        # Drop all null columns 
        self.df = self.df.select([col for col in self.df.columns if not self.df[col].is_null().all()]) 

        self.path_to_tracciato = os.path.join(path_to_main_folder, f"METADATI/{self.df_name}_Tracciato_{self.year}.csv")
        
        # Check if the tracciato exists
        if os.path.exists(self.path_to_tracciato):
            self.tracciato_df = pl.read_csv(self.path_to_tracciato)
            categ_cols = ["category_1", "category_2", "category_3", "category_4"]
            missing_columns = [col for col in categ_cols if col not in self.tracciato_df.columns]
            if missing_columns:
                raise ValueError(f"The following required columns are missing from the DataFrame: {missing_columns}")
            self.attribute_categories = pl.concat(
                [self.tracciato_df[col] for col in categ_cols]
            ).unique().to_list()
        else:
            warnings.warn(f"File not found: {self.path_to_tracciato}")

    def _read_html(self, path_to_html: str) -> List:
        """Read an HTML file and extract the table data."""
        with open(path_to_html, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")

        rows = []
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            row_data = [cell.get_text(strip=True) for cell in cells]
            if row_data:
                rows.append(row_data)
        return rows
    
    def get_attributes_by_categories(
            self,
            cat_1: str,
            cat_2: str | None = None,
            cat_3: str | None = None,
            cat_4: str | None = None,
            how: Literal["and", "or"] = "and",
            print_output: bool = True
        ) -> pd.DataFrame | pl.DataFrame:
        """
        Filter rows of `tracciato_df` according to the presence of up-to-three category
        labels in the columns 'category_1', 'category_2', 'category_3', 'category_4'.

        Parameters
        ----------
        cat_1, cat_2, cat_3, cat_4 : str | None
            Category values to search for (cat_2 / cat_3 /cat_4 may be omitted).
        condition : {'and', 'or'}
            * 'and' → every non-None category must appear at least once
            across the three category columns (order doesn’t matter).
            * 'or'  → at least one of the given categories must appear.

        Returns
        -------
        pl.DataFrame
            The filtered frame (also printed to stdout).
        """
        cats = [c for c in (cat_1, cat_2, cat_3, cat_4) if c is not None]

        cols = ["category_1", "category_2", "category_3", "category_4"]

        how = how.lower()
        if how == "or":
            # any column contains any of the cats
            filt = pl.any_horizontal([pl.col(col).is_in(cats) for col in cols])

        elif how == "and":
            # for each cat: it must appear in some of the three columns
            cat_exprs = [
                pl.any_horizontal([pl.col(col) == cat for col in cols])
                for cat in cats
            ]
            filt = reduce(operator.and_, cat_exprs)
        else:
            raise ValueError('condition must be either "and" or "or"')

        result = self.tracciato_df.filter(filt)

        num_ord = "num. ordine"
        # print and return
        if print_output:
            print(f"{len(result)} attributes matching the search criteria")
            print(f"Results for {cat_1}{' ' + how if cat_2 is not None else ''}{' ' + cat_2 if cat_2 is not None else ''}{' ' + how if cat_3 is not None else ''}{' ' + cat_3 if cat_3 is not None else ''}{' ' + how if cat_4 is not None else ''}{' ' + cat_4 if cat_4 is not None else ''}:\n")

            print("n°   Attribute\tDescription")
            print("-----------------------------------------------------")
            for row in result.iter_rows(named=True):
                print(f'{row[num_ord]}{"    " if len(str(row[num_ord])) == 1 else "   " if len(str(row[num_ord])) == 2 else "  "}{row["Acronimovariabile"]}:\t{row["Denominazione Variabile"]}')

        if self.get_polars:
            return result
        else:
            return result.to_pandas()

    def get_attribute_metadata(
            self,
            attribute: int | str, 
            print_output: bool = False
        ) -> Dict | None:
        """
        Get the encoding and description of a attribute in the ISTAT dataset.
        Parameters
        ----------
        attribute : int or str
            The attribute number or name to get the encoding for.
        print_output : bool
            If True, print the encoding table.
        Returns
        -------
        dict
            A dictionary mapping the encoding to the description.
        """
        attribute_name = None
        if isinstance(attribute, str):
            # Convert attribute name to number
            # try:
            attribute_name = attribute
            attribute = self.tracciato_df.filter(pl.col("Acronimovariabile") == attribute).select("num. ordine").to_numpy()[0][0]
            # except:
                # return None
        elif isinstance(attribute, int):
            attribute_name = self.tracciato_df.filter(pl.col("num. ordine") == attribute).select("Acronimovariabile").to_numpy()[0][0]
            
        path_to_file = os.path.join(self.path_to_main_folder, f"METADATI/Classificazioni/{self.df_name}_Classificazione_{self.year}_var{attribute}.html")

        if not os.path.exists(path_to_file):
            print(f"File {path_to_file} does not exist.\n\nAttribute n° {attribute} ({attribute_name}) may be of numerical type.")
            return None
        else:
            rows = self._read_html(path_to_file)
            
            dict = {int(row[0]): row[1] for row in rows[1:]}

            if print_output:
                print("Attribute:\n  ",attribute_name)
                print("Description:\n  ",self.tracciato_df.filter(pl.col("Acronimovariabile")==attribute_name)["Denominazione Variabile"].item())
                print("Encod.\tLabel")
                for key, value in dict.items():
                        print(f"""{key}\t{value}""")

            return dict
    
    def _get_encoded_value(self, col: str, val: Any):
        if isinstance(val, int):
            return val
        elif isinstance(val, str):
            encoding_dict = self.get_attribute_metadata(col)
            encoding_list = [key for key, v in encoding_dict.items() if v == val]
            if len(encoding_list)!=1:
                raise ValueError(f"None or more than one encoded value associated to {val} in column {col}.")
            else:
                return encoding_list[0]
        elif isinstance(val, list):
            if all(isinstance(n, int) for n in val):
                 return val
            elif all(isinstance(n, str) for n in val):
                encoding_dict = self.get_attribute_metadata(col)
                encoding_list = []
                for el in val:    
                    encoding_list = [key for el in val for key, v in encoding_dict.items() if v == el]
                return encoding_list
            else:
                raise TypeError(f"Invalid or mixed types in {val} in column {col}.")
        else:
            raise TypeError(f"Invalid type for {val} in column {col}.")

    def _expr(self, triplets: List[Tuple[str, str, Any]]) -> pl.Expr:
        """
        Build a single Polars expression that AND-combines a list of
        (column, operator, value) conditions.

        Special case:
            - If operator is "==", the expression becomes
            `pl.col(col).is_in(value_list)` so that a scalar or list‐like
            value can match any of the provided values.
        """
        exprs = []
        
        # Check if triplets is a list of tuples with 3 elements
        if not isinstance(triplets, list) or not all(isinstance(t, tuple) and len(t) == 3 for t in triplets):
            raise TypeError("Expected a list of (col, op, val) triplets.")

        for col, op, val in triplets:
            if op not in self._OPS:
                raise ValueError(
                    f"Unsupported operator '{op}' in condition ({col}, {op}, {val}). "
                    f"Supported operators: {list(self._OPS)}"
                )

            # Encode value(s) first
            enc_val = self._get_encoded_value(col, val)

            if op == "==":
                # Ensure enc_val is iterable for `is_in`
                if not isinstance(enc_val, (list, tuple, set)):
                    enc_val = [enc_val]
                exprs.append(pl.col(col).is_in(enc_val))
            else:
                exprs.append(self._OPS[op](pl.col(col), enc_val))

        return reduce(lambda a, b: a & b, exprs) if exprs else pl.lit(True)
    
    Triplet       = Tuple[str, str, Any]
    TripletGroup  = Sequence[Triplet]               # AND-ed together
    ConditionsT   = Union[TripletGroup,             # flat list  → all AND
                        Sequence[TripletGroup]]   # list of lists → OR of AND-groups
    def filter(
        self,
        conditions: ConditionsT,
        df: pd.DataFrame | pl.DataFrame | None = None,
        get_polars = False
    ) -> pd.DataFrame | pl.DataFrame:
        """
        Filter a Polars DataFrame with arbitrarily complex boolean logic.

        Parameters
        ----------
        conditions
            • A *flat* sequence of (col, op, val) → all AND-ed **(back-compat)**  
            Example:  [("age", ">", 30), ("country", "==", "US")]

            • A sequence *of sequences* → inner lists are AND-ed, outer level OR-ed  
            Example:
                [
                  [("ETAMi",">=",7),("BMI","<=",3)],  # Adults (age>=18) AND BMI==[1,2,3]
                                                      # OR
                  [("ETAMi","<",7),("BMIMIN","==",1)] # minors (age<18) AND BMIMIN==1
                ]
            expresses:  (age>=18 AND BMI<=3)  OR  (age<18 AND BMIMIN==1)

        Returns
        -------
        pl.DataFrame
            Rows that satisfy the combined condition(s).
        """
        if df is None:
            df = self.df
        if not conditions:
            return df

        # Transform df to polars if needed
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        # Normalise: make sure it always works with a list of AND-groups
        if isinstance(conditions[0], tuple):          # user gave a flat list
            conditions = [conditions]                 # wrap in a single group

        # Guard against malformed inputs early
        if not all(isinstance(g, Sequence) and g and isinstance(g[0], tuple)
                for g in conditions):
            raise TypeError(
                "Expected a list of (col, op, val) triplets *or* "
                "a list of such lists."
            )

        # Build Polars expressions
        #   • self._expr(group)  -> AND of one group
        #   • reduce(|)          -> OR across groups
        and_groups = [self._expr(list(group)) for group in conditions]
        combined_expr = reduce(operator.or_, and_groups)

        if self.get_polars or get_polars:
            return df.filter(combined_expr)
        else:
            return df.to_pandas()


    def joint_distribution(
            self,
            attrs: List[str],
            df: pd.DataFrame | pl.DataFrame | None = None,
            conditions: ConditionsT = None,
            *,
            normalise: bool = True,
            keep_counts: bool = True,
            get_attr_metadata: bool = False
        ) -> Tuple[pd.DataFrame | pl.DataFrame, Optional[Dict[str, Dict[str, Any]]]]:
        """
        Compute the joint distribution of `attrs` in a Polars DataFrame,
        honouring the comparison conditions supplied.

        Parameters
        ----------
        attrs       : list[str]  – columns whose joint distribution is required.
        conditions  : list[(col, op, val)], optional
                    op ∈ {'==','!=','>','>=','<','<=','in','not in'}
        normalise   : if True, append 'prob' = count / total.
        keep_counts : if False, drop the raw 'count' column.

        Returns
        -------
        joint : pl.DataFrame
            columns: attrs + ['count'] + ['prob' (if normalise)]
        meta  : dict | None
            {attr: embed(attr)} if `embed` provided, else None.

        Example
        -------
            mde = ISTATMicrodataExtractor("AVQ_2023_IT")
            joint, meta = mde.joint_distribution(
                attrs=["SESSO", "STCIVMi"],
                conditions=[
                    ("ANNO", "==", 2023),
                    ("SESSO", "!=", 1),
                    ("ETAMi", ">=", 7)
                ],
                how="and",
                normalise=True,
            )
        """
        if df is None:
            df = self.df

        # Transform df to polars if needed
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        # Optional filtering
        if conditions:
            df = self.filter(conditions, df=df, get_polars=True)

        # Aggregate to joint counts
        joint = (
            df.group_by(attrs)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        if normalise and joint.height > 0:
            total = joint["count"].sum()
            joint = joint.with_columns((pl.col("count") / total).alias("prob"))

        if not keep_counts:
            joint = joint.drop("count")

        
        if get_attr_metadata:
            # Optional metadata
            embed = self.get_attribute_metadata
            meta = {v: embed(v) for v in attrs} if embed else None
            if self.get_polars:
                return joint, meta
            else:
                return joint.to_pandas(), meta
        else:
            if self.get_polars:
                return joint
            else:
                return joint.to_pandas()

 
    def pair_family_members(
        self,
        rules: List[Dict[str, Any]],
        attrs: Optional[List[str]] = None,
        *,
        filter_df_rules: List[Dict[str, Any]] = None,
        family_key: str = "PROFAM",
        id_col: str = "PROIND",
    ) -> pl.DataFrame:
        """
        Pair individuals within the same household according to user rules
        and optionally return attributes for each person.

        Parameters
        ----------
        rules      : list of dicts with keys
            - 'ind1' : triplet list for individual-1 filter
            - 'ind2' : triplet list for individual-2 filter
            - optional 'name' and 'extra_pair_cond'
        attrs      : list of str, optional
            Extra columns to return for each person.
            e.g. ['ETAMi','SESSO'] → ETAMi_ind1, ETAMi_ind2,...
        filter_df_rules  : list of dicts, optional
            If provided, filter the DataFrame before pairing.
        family_key : str
            household identifier column.
        id_col     : str
            individual identifier column.
                    

        Returns
        -------
        pl.DataFrame with columns
            rule | family_key | PROIND_1 | PROIND_2 | [attrs *_ind1/_ind2 ...]
        """        
        if self.df_name == "HBS":
            print("The method pair_family_members is not available for the HBS dataset, as each row represents a family and the demographic attributes are explicitly available for all family members (e.g. sesso_1, pnasc_1, staciv_1, sesso_2, pnasc_2, staciv_2, etc...).")
            return
        
        df = self.filter(filter_df_rules, get_polars=True)
        all_pairs = []
        if attrs is None:
            attrs = []
        else:
            attrs = [el for el in attrs if el in df.columns]

        # pre-build attribute tables for later joins
        if attrs:
            ind1_attr_tbl = (
                df.select([family_key, id_col] + attrs)
                .rename(
                    {id_col: "PROIND_1", **{c: f"{c}_ind1" for c in attrs}}
                )
            )
            ind2_attr_tbl = (
                df.select([family_key, id_col] + attrs)
                .rename(
                    {id_col: "PROIND_2", **{c: f"{c}_ind2" for c in attrs}}
                )
            )

        for r_idx, rule in enumerate(rules, start=1):
            label = rule.get("name", f"rule_{r_idx}")

            # candidate sets
            cand1 = (
                df.filter(self._expr(rule["ind1"]))
                .select([family_key, id_col])
                .rename({id_col: "PROIND_1"})
            )
            cand2 = (
                df.filter(self._expr(rule["ind2"]))
                .select([family_key, id_col])
                .rename({id_col: "PROIND_2"})
            )

            # cartesian join within household
            pairs = cand1.join(cand2, on=family_key, how="inner")

            # optional cross-row predicate
            xpair = rule.get("extra_pair_cond")
            if xpair is not None and not pairs.is_empty():
                # full row copies with suffixes for predicate evaluation
                cols_all = list(df.columns)
                left  = (
                    df.select(cols_all)
                    .rename({c: f"{c}_left"  for c in cols_all
                            if c not in (family_key, id_col)})
                )
                right = (
                    df.select(cols_all)
                    .rename({c: f"{c}_right" for c in cols_all
                            if c not in (family_key, id_col)})
                )
                tmp = (
                    left.join(right, on=family_key, how="inner")
                        .rename({id_col: "PROIND_1", f"{id_col}_right": "PROIND_2"})
                )

                l = lambda col: pl.col(f"{col}_left")
                r = lambda col: pl.col(f"{col}_right")
                pairs = (
                    pairs.join(tmp, on=[family_key, "PROIND_1", "PROIND_2"], how="left")
                        .filter(xpair(l, r))
                        .select([family_key, "PROIND_1", "PROIND_2"])
                )

            # remove self-pairs & order ids
            pairs = (
                pairs.filter(pl.col("PROIND_1") != pl.col("PROIND_2"))
                    .with_columns(
                        pl.when(pl.col("PROIND_1") < pl.col("PROIND_2"))
                        .then(pl.struct(["PROIND_1", "PROIND_2"]))
                        .otherwise(pl.struct(["PROIND_2", "PROIND_1"]))
                        .alias("ordered")
                    )
                    .select([
                        pl.lit(label).alias("rule"),
                        family_key,
                        pl.col("ordered").struct.field("PROIND_1").alias("PROIND_1"),
                        pl.col("ordered").struct.field("PROIND_2").alias("PROIND_2"),
                    ])
                    .unique()
            )

            # attach attributes if requested
            if not pairs.is_empty():
                if attrs:
                    pairs = (
                        pairs.join(ind1_attr_tbl, on=[family_key, "PROIND_1"], how="left")
                             .join(ind2_attr_tbl, on=[family_key, "PROIND_2"], how="left")
                    )

                all_pairs.append(pairs)

        # stack all rules together
        if self.get_polars:
            return pl.concat(all_pairs) if all_pairs else pl.DataFrame()
        else:
            return pl.concat(all_pairs).to_pandas() if all_pairs else pd.DataFrame()

if __name__ == "__main__":

    # mde = ISTATMicrodataExtractor(df_name="AVQ", year="2023", get_polars=True)
    # mde.load_data("Replica/AVQ_2023_IT")
    mde = ISTATMicrodataExtractor(df_name="HBS", year="2023", get_polars=True)
    mde.load_data("Replica/HBS_2023_IT")

    mde.get_attribute_metadata("c_ateco_1")
    # Filter returns adults (age>=18) with BMI==[1,2,3] and minors (age<18) with BMIMIN==1
    df_filt=mde.filter([[("ETAMi",">=",7),("BMI","<=",3)],[("ETAMi","<",7),("BMIMIN","==",1)]])

    joint = mde.joint_distribution(
                attrs=["FREQSPO", "BMI"],
                conditions=[("ETAMi", ">", 7)], # Maggiorenni
                normalise=True,
            )
    joint_min = mde.joint_distribution(
                attrs=["FREQSPO", "BMIMIN"],
                conditions=[("ETAMi", "<=", 7)], # Minorenni
                normalise=True,
            )
    
    regioni = ["Valle d\'Aosta","Piemonte","Lombardia"]
    filter_df_rules = [("REGMf", "==", regioni)]
    mother_child_rules = [
        {"name": "RELPAR_6",
        "ind1": [("RELPAR", "==", 6)],
        "ind2": [("RELPAR", "==", 1), ("SESSO", "==", 2)]},
        {"name": "RELPAR_7_1",
        "ind1": [("RELPAR", "==", 7)],
        "ind2": [("RELPAR", "==", 1), ("SESSO", "==", 2)]},
        {"name": "RELPAR_7_2",
        "ind1": [("RELPAR", "==", 7)],
        "ind2": [("RELPAR", "==", 2), ("SESSO", "==", 2)]},
        {"name": "RELPAR_4_1",
        "ind1": [("RELPAR", "==", 1)],
        "ind2": [("RELPAR", "==", 4), ("SESSO", "==", 2)]},
        {"name": "RELPAR_5_2",
        "ind1": [("RELPAR", "==", 2)],
        "ind2": [("RELPAR", "==", 5), ("SESSO", "==", 2)]},
        {"name": "RELPAR_5_2",
        "ind1": [("RELPAR", "==", 3)],
        "ind2": [("RELPAR", "==", 5), ("SESSO", "==", 2)]},
        {"name": "RELPAR_5_2",
        "ind1": [("RELPAR", "==", 3)],
        "ind2": [("RELPAR", "==", 5), ("SESSO", "==", 2)]},
        {"name": "Fallback_RELPAR_6_2",
        "ind1": [("RELPAR", "==", 6)],
        "ind2": [("RELPAR", "==", 2), ("SESSO", "==", 2)]},
    ]

    relpar_col = "RELPAR"
    partners_rules = [
        {"name": "REPLAR_1_2",
         "ind1": [(relpar_col,"==",1)],
         "ind2": [(relpar_col,"==",2)]},
         {"name": "REPLAR_1_3",
         "ind1": [(relpar_col,"==",1)],
         "ind2": [(relpar_col,"==",3)]},
         {"name": "RELPAR_4_5",
          "ind1": [(relpar_col,"==",4)],
          "ind2": [(relpar_col,"==",5)]},
         {"name": "RELPAR_6_8",
          "ind1": [(relpar_col,"==",6)],
          "ind2": [(relpar_col,"==",8)]},
          {"name": "RELPAR_7_8",
          "ind1": [(relpar_col,"==",7)],
          "ind2": [(relpar_col,"==",8)]},
          {"name": "RELPAR_6_9",
          "ind1": [(relpar_col,"==",6)],
          "ind2": [(relpar_col,"==",9)]},
          {"name": "RELPAR_7_9",
          "ind1": [(relpar_col,"==",7)],
          "ind2": [(relpar_col,"==",9)]},
          {"name": "RELPAR_12_14",
          "ind1": [(relpar_col,"==",12)],
          "ind2": [(relpar_col,"==",14)]},
          {"name": "RELPAR_12_15",
          "ind1": [(relpar_col,"==",12)],
          "ind2": [(relpar_col,"==",15)]},
          {"name": "RELPAR_13_14",
          "ind1": [(relpar_col,"==",13)],
          "ind2": [(relpar_col,"==",14)]},
          {"name": "RELPAR_13_15",
          "ind1": [(relpar_col,"==",13)],
          "ind2": [(relpar_col,"==",15)]},          
        ]   
    
    attrs_pair = ["ETAMi","SESSO"]
    partners_df = mde.pair_family_members(mother_child_rules, attrs=attrs_pair, filter_df_rules=filter_df_rules)

    attrs_joint = ["ETAMi_ind1","ETAMi_ind2"]#, "SESSO_ind1", "SESSO_ind2"]
    joint_partners = mde.joint_distribution(attrs=attrs_joint,df=partners_df)
    print(partners_df.head())
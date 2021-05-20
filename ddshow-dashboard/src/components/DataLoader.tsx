import AddIcon from "@material-ui/icons/Add";
import React from "react";
import { Redirect } from "react-router-dom";
import {
    DialogContentText,
    Dialog,
    DialogContent,
    DialogTitle,
    Fab,
    DialogActions,
    Snackbar,
} from "@material-ui/core";
import { Alert, AlertTitle } from "@material-ui/lab";

export type DataLoaderProps = {
    on_file_load: (data: string) => void;
};

export type LoadStatus = {
    data_load_status?: {
        status: LoaderStatus;
        message: string | null;
    };
};

export enum LoaderStatus {
    Success,
    Failure,
    None,
}

type DataLoaderState = {
    open: boolean;
    redirect: boolean;
    status: LoaderStatus;
    message: string | null;
    input_ref: React.RefObject<HTMLInputElement>;
    loaded_data?: string;
};

export default class DataLoader extends React.Component<
    DataLoaderProps,
    DataLoaderState
> {
    state = {
        open: true,
        redirect: false,
        status: LoaderStatus.None,
        message: null,
        input_ref: React.createRef<HTMLInputElement>(),
    };

    onChange = (files: FileList): void => {
        if (files.length === 1) {
            const file = files[0];

            file.text().then(text => {
                this.props.on_file_load(text);
            });

            this.setState({
                open: false,
                redirect: true,
                status: LoaderStatus.Success,
            });
        } else {
            this.setState({
                status: LoaderStatus.Failure,
                message: "no files were loaded",
            });
        }
    };

    render() {
        return (
            <>
                {/* TODO: Support drag & drop files */}
                <Dialog
                    open={this.state.open}
                    aria-labelledby="form-dialog-title"
                    maxWidth="lg"
                >
                    <DialogTitle id="form-dialog-title">
                        Load Dataflow Data
                    </DialogTitle>

                    <DialogContent>
                        <DialogContentText>
                            Load a dataflow's generated <code>.json</code> file
                            to analyze and visualize it
                        </DialogContentText>
                    </DialogContent>

                    <DialogActions>
                        <Fab component="label">
                            <AddIcon />

                            <input
                                type="file"
                                accept=".json"
                                onChange={event => {
                                    if (event.target.files) {
                                        this.onChange(event.target.files);
                                    } else {
                                        console.log("errored");
                                        this.setState({
                                            status: LoaderStatus.Failure,
                                            message: "No file was provided",
                                        });
                                    }
                                }}
                                hidden
                                ref={this.state.input_ref}
                            />
                        </Fab>
                    </DialogActions>
                </Dialog>

                {this.state.status === LoaderStatus.Failure && (
                    <Snackbar autoHideDuration={6000}>
                        <Alert severity="error">
                            <AlertTitle>Error</AlertTitle>

                            {this.state.message
                                ? this.state.message
                                : "Failed to load data file"}
                        </Alert>
                    </Snackbar>
                )}

                {this.state.redirect && (
                    <Redirect
                        push
                        to={{
                            pathname: "/overview",
                            state: {
                                data_load_status: {
                                    status: this.state.status,
                                    message: this.state.message,
                                },
                            },
                        }}
                    />
                )}
            </>
        );
    }
}

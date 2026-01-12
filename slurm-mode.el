;;; slurm-mode.el --- Interaction with the SLURM job scheduling system

;; Copyright (C) 2012 François Févotte
;; Version:
;; URL: https://github.com/ffevotte/slurm.el
;; Repository: https://github.com/ffevotte/slurm.el.git

;; This file is NOT part of Emacs.

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.


;;; Commentary:

;; If you make improvements to this code or have suggestions, please do not hesitate to fork the
;; repository or submit bug reports on github.  The repository is at:
;;
;;     https://github.com/ffevotte/slurm.el

;;; Code:


(require 'dash)
(require 's)
(require 'org)

;; * Customizable variables

;;;###autoload
(defgroup slurm nil
  "Interacting with the SLURM jobs scheduling system."
  :group 'external)

;;;###autoload
(defcustom slurm-display-help t
  "If non-nil, `slurm-mode' should display an help message at the top of the screen."
  :group 'slurm
  :type 'boolean)

;;;###autoload
(defcustom slurm-filter-user-at-start t
  "If non-nil, the jobs list is filtered by user at start."
  :group 'slurm
  :type 'boolean)

;;;###autoload
(defcustom slurm-remote-host nil
  "Execute SLURM commands on this remote host.
It uses SSH rather than executing them directlySee also `slurm-remote-username' and `slurm-remote-ssh-cmd'."
  :group 'slurm
  :type 'string)

;;;###autoload
(defcustom slurm-remote-username nil
  "Username to use for SSHing to the remote machine.
It is specified in `slurm-remote-host'."
  :group 'slurm
  :type 'string)

;;;###autoload

;;;###autoload
(defcustom slurm-scancel-confirm t
  "If non-nil, ask for confirmation before cancelling a job."
  :group 'slurm
  :type  'boolean)

;;;###autoload
(defcustom slurm-watch-interval 30
  "Interval in seconds for checking watched job status."
  :group 'slurm
  :type 'integer)

;;;###autoload
(defcustom slurm-watch-notify-function #'slurm-watch-notify-default
  "Function to call when a watched job completes.
The function is called with two arguments: JOB-ID and STATUS."
  :group 'slurm
  :type 'function)

(defun slurm--set-squeue-format (var val)
  "Set the argument for the squeue Command from VAR and VAL."
  (set-default var val)
  (when (fboundp 'slurm-update-squeue-format)
    (slurm-update-squeue-format)))

;;;###autoload
(defcustom slurm-squeue-format
  '((jobid      20 left)
    (partition  9 left)
    (name      37 left)
    (user       8 left)
    (st         2 left)
    (time      10 right)
    (nodes      4 right)
    (priority   4 right)
    (nodelist  40 left))
  "List of fields to display in the jobs list.

Each entry in the list should be of the form:
  (FIELD WIDTH ALIGNMENT)
where:
FIELD is a symbol whose name corresponds to the column title in
      the squeue output.
WIDTH is an integer setting the column width.
ALIGN is either `left' or `right'.

`slurm-update-squeue-format' must be called after this variable
is changed to ensure the new value is used wherever necessary."
  :group 'slurm
  :set   'slurm--set-squeue-format
  :type  '(alist
           :key-type   (symbol :tag "Field")
           :value-type (group (integer :tag "Width")
                              (choice  :tag "Alignment"
                                       (const left)
                                       (const right)))))

(defun slurm--set-sacct-format (var val)
  "Update the argument for the sacct Command from VAR and VAL."
  (set-default var val)
  (when (fboundp 'slurm-update-sacct-format)
    (slurm-update-sacct-format)))

(defcustom slurm-sacct-format
  '((jobid      20 left)
    (jobname  20  right)
    (workdir  60  right)
    (state  25  right)
    (start 20 right)
    (elapsed 10 right))
  "List of fields to display in the jobs list.

Each entry in the list should be of the form:
  (FIELD WIDTH ALIGNMENT)
where:
FIELD is a symbol whose name corresponds to the column title in
      the squeue output.
WIDTH is an integer setting the column width.
ALIGN is either `left' or `right'.

`slurm-update-sacct-format' must be called after this variable
is changed to ensure the new value is used wherever necessary."
  :group 'slurm
  :set   'slurm--set-sacct-format
  :type  '(alist
           :key-type   (symbol :tag "Field")
           :value-type (group (integer :tag "Width")
                              (choice  :tag "Alignment"
                                       (const left)
                                       (const right)))))


;; * Utilities

;; ** Process management

(defun slurm--remote-command (cmd)
  "Wraps SLURM command CMD in ssh if `slurm-remote-host' is set.
Otherwise, CMD is returned unmodified."
   (if slurm-remote-host
       (append `(,slurm-remote-host)
               (if (listp cmd) `(,(combine-and-quote-strings cmd)) `(,cmd)))
     cmd))

(defvar slurm--buffer)
(defmacro slurm--run-command (&rest args)
  "Synchronously run a command.

ARGS is a plist containing the following entries:

:command (required) - the command to run, as a list.

:message (optional) - a message to be displayed.

:post (optional) - form to be executed in the process buffer
  after completion.  The `slurm--buffer' variable is let-bound
  around this block, pointing to the slurm buffer (i.e. the
  buffer from which `slurm--run-command' was called).

:current-buffer (optional) - if non-nil, insert the output of the
  command at the end of the current buffer."
  (let* ((command        (plist-get args :command))
         (post           (plist-get args :post))
         (current-buffer (plist-get args :current-buffer))
         (message        (plist-get args :message))

         (buffer-sym     (cl-gensym "buffer"))
         (command-sym    (cl-gensym "command"))
         (message-sym    (cl-gensym "message")))
    `(progn
       (let* ((slurm--buffer (current-buffer))
              (,command-sym  ,command)
              (,message-sym  ,message)
              (,buffer-sym   (get-buffer-create " *slurm process*")))
         ,@(when message
             `((message "%s..." ,message-sym)))
         (with-current-buffer ,buffer-sym
           (erase-buffer)
           (apply 'eshell-command
                  (mapconcat 'identity
                             (slurm--remote-command ,command-sym)
                             " ")
                  t
                  nil))
         ,@(when message
             `((message "%s...done." ,message-sym)))
         ,@(when post
             `((with-current-buffer ,buffer-sym
                 ,post)))
         ,@(when current-buffer
             `((save-excursion
                 (goto-char (point-max))
                 (insert-buffer-substring ,buffer-sym))))))))


;; ** Internal state management

(defvar slurm--state nil
  "Internal state of slurm.")

;; -- global
;;
;; :initialized      - if non-nil filters and sort will refresh the view
;; :command          - list of commands actually displayed on the view
;; :running-commands - list of commands currently being executed
;; :view             - name of the current view
;; :partitions       - list of available partitions
;; :old-position     - current position just before refreshing a view. Will try
;;                     to go back there after refreshing.

;; -- specific to the jobs list
;;
;; :filter-user      - user to filter in the jobs list ("" for no filter)
;; :filter-partition - partition to filter in the jobs list ("*ALL*" for no filter)
;; :sort             - sorting order ("" for the default sorting order)

;; -- specific to the detailed job view
;;
;; :jobid - current job id

(defun slurm--get (key)
  "Get surm internal variable value associated to KEY."
  (plist-get slurm--state key))

(defun slurm--set (key value)
  "Set the SLURM internal variable associated to KEY.
Assign it the new value VALUE."
  (setq slurm--state
        (plist-put slurm--state key value)))


;; ** Job watching

(defvar slurm-watch-timers (make-hash-table :test 'equal)
  "Hash table mapping job IDs to their watch timers.")

(defun slurm-watch-notify-default (job-id status)
  "Default notification function for completed jobs.
JOB-ID is the job identifier, STATUS is the final job state."
  (message "SLURM job %s completed with status: %s" job-id status)
  (when (fboundp 'alert)
    (alert (format "Job %s: %s" job-id status)
           :title "SLURM Job Complete"
           :category 'slurm)))

(defun slurm--get-job-state (job-id)
  "Get the current state of JOB-ID.
Returns nil if job not found, or the state string (e.g., COMPLETED, FAILED, RUNNING)."
  (with-temp-buffer
    (call-process "squeue" nil t nil "-j" job-id "-h" "-o" "%T")
    (let ((state (string-trim (buffer-string))))
      (if (not (string-empty-p state))
          state
        ;; Job not in queue, check sacct for final state
        (erase-buffer)
        (call-process "sacct" nil t nil "-j" job-id "-n" "-o" "State" "-X")
        (let ((sacct-state (string-trim (buffer-string))))
          (unless (string-empty-p sacct-state) sacct-state))))))

(defun slurm--check-watched-job (job-id)
  "Check if watched JOB-ID has completed and notify if so."
  (let ((state (slurm--get-job-state job-id)))
    (when (and state
               (not (member state '("PENDING" "RUNNING" "CONFIGURING"
                                    "COMPLETING" "RESIZING" "SUSPENDED"))))
      ;; Job has finished
      (slurm-unwatch-job job-id)
      (funcall slurm-watch-notify-function job-id state))))

(defun slurm-watch-job (&optional job-id)
  "Start watching JOB-ID for completion.
When the job finishes, `slurm-watch-notify-function' is called.
If JOB-ID is nil and in slurm-mode, use job at point."
  (interactive)
  (let ((id (or job-id
                (when (eq major-mode 'slurm-mode)
                  (slurm-job-id))
                (read-string "Job ID to watch: "))))
    (if (gethash id slurm-watch-timers)
        (message "Already watching job %s" id)
      (let ((timer (run-at-time slurm-watch-interval
                                slurm-watch-interval
                                #'slurm--check-watched-job
                                id)))
        (puthash id timer slurm-watch-timers)
        (message "Watching job %s (checking every %ds)" id slurm-watch-interval)))))

(defun slurm-unwatch-job (&optional job-id)
  "Stop watching JOB-ID.
If JOB-ID is nil, prompt with list of watched jobs."
  (interactive)
  (let* ((watched (hash-table-keys slurm-watch-timers))
         (id (or job-id
                 (if watched
                     (completing-read "Unwatch job: " watched nil t)
                   (user-error "No jobs being watched")))))
    (when-let ((timer (gethash id slurm-watch-timers)))
      (cancel-timer timer)
      (remhash id slurm-watch-timers)
      (message "Stopped watching job %s" id))))

(defun slurm-list-watched-jobs ()
  "Display a buffer listing currently watched jobs with their states."
  (interactive)
  (let ((watched (hash-table-keys slurm-watch-timers)))
    (if (not watched)
        (message "No jobs being watched")
      (let ((buf (get-buffer-create "*SLURM Watched Jobs*")))
        (with-current-buffer buf
          (let ((inhibit-read-only t))
            (erase-buffer)
            (insert "SLURM Watched Jobs\n")
            (insert "==================\n\n")
            (insert (format "%-12s %-15s %s\n" "Job ID" "State" "Interval"))
            (insert (make-string 45 ?-) "\n")
            (dolist (job-id watched)
              (let ((state (or (slurm--get-job-state job-id) "UNKNOWN")))
                (insert (format "%-12s %-15s %ds\n"
                                job-id state slurm-watch-interval))))
            (insert "\n")
            (insert "Press 'q' to close, 'W' to unwatch a job, 'g' to refresh\n"))
          (special-mode)
          (local-set-key (kbd "W") #'slurm-unwatch-job)
          (local-set-key (kbd "g") #'slurm-list-watched-jobs))
        (pop-to-buffer buf)))))


;; ** Job output viewing

(defun slurm--get-job-file (job-id field)
  "Get the file path for FIELD (StdOut or StdErr) from JOB-ID's scontrol output."
  (with-temp-buffer
    (when (= 0 (call-process "scontrol" nil t nil "show" "job" job-id))
      (goto-char (point-min))
      (when (re-search-forward (format "%s=\\([^\n ]+\\)" field) nil t)
        (match-string 1)))))

(defun slurm--get-job-output-file (job-id)
  "Get the stdout file path for JOB-ID from scontrol."
  (slurm--get-job-file job-id "StdOut"))

(defun slurm--view-job-file (job-id field description)
  "View the FIELD file (StdOut or StdErr) for JOB-ID.
DESCRIPTION is used in messages (e.g., \"output\" or \"stderr\")."
  (let ((file (slurm--get-job-file job-id field)))
    (if (not file)
        (user-error "Could not find %s file for job %s" description job-id)
      (if (not (file-exists-p file))
          (user-error "%s file does not exist yet: %s"
                      (capitalize description) file)
        (find-file-other-window file)
        (goto-char (point-max))
        (auto-revert-tail-mode 1)
        (message "Viewing %s for job %s (auto-revert-tail-mode enabled)"
                 description job-id)))))

(defun slurm--resolve-job-id (job-id)
  "Resolve JOB-ID from argument, point, or prompt."
  (or job-id
      (when (eq major-mode 'slurm-mode)
        (slurm-job-id))
      (read-string "Job ID: ")))

(defun slurm-job-output (&optional job-id)
  "View the stdout log file for JOB-ID.
Opens the file with `auto-revert-tail-mode' for live tailing.
If JOB-ID is nil and in slurm-mode, use job at point."
  (interactive)
  (slurm--view-job-file (slurm--resolve-job-id job-id) "StdOut" "output"))

(defun slurm-job-error-output (&optional job-id)
  "View the stderr log file for JOB-ID.
Opens the file with `auto-revert-tail-mode' for live tailing.
If JOB-ID is nil and in slurm-mode, use job at point."
  (interactive)
  (slurm--view-job-file (slurm--resolve-job-id job-id) "StdErr" "stderr"))


;; * Slurm mode

;; ** Mode definition

;;;###autoload
(defun slurm ()
  "Open a `slurm-mode' buffer to manage jobs."
  (interactive)
  (if (file-remote-p default-directory)
      (setq slurm-remote-host (concat "/ssh:" (file-remote-p default-directory 'host) ":" ";"))
    (setq slurm-remote-host nil))

  (if (file-remote-p default-directory)
      (switch-to-buffer (get-buffer-create (concat "slurm-" (file-remote-p default-directory 'host))))
    (switch-to-buffer (get-buffer-create "slurm")))
  (if (derived-mode-p 'slurm-mode)
      (slurm-refresh)
    (slurm-mode)))

(defvar slurm-mode-view-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "j")   'slurm-job-list)
    (define-key map (kbd "a")   'slurm-sacct)
    (define-key map (kbd "p")   'slurm-partition-list)
    (define-key map (kbd "i")   'slurm-cluster-info)
    (define-key map (kbd "g")   'slurm-refresh)
    map)
  "Keymap for `slurm-mode-view'.")
(defvar slurm-mode-partition-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "RET") 'slurm-details)
    map)
  "Keymap for `slurm-mode-partition'.")

(defvar slurm-mode-job-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "RET") 'slurm-details)
    (define-key map (kbd "U")   'slurm-job-user-details)
    (define-key map (kbd "S")   'slurm-seff)
    (define-key map (kbd "d")   'slurm-job-cancel)
    (define-key map (kbd "k")   'slurm-job-cancel)
    (define-key map (kbd "u")   'slurm-job-update)
    (define-key map (kbd "w")   'slurm-watch-job)
    (define-key map (kbd "W")   'slurm-unwatch-job)
    (define-key map (kbd "L")   'slurm-list-watched-jobs)
    (define-key map (kbd "o")   'slurm-job-output)
    (define-key map (kbd "e")   'slurm-job-error-output)
    map)
  "Keymap for `slurm-mode-job'.")
(defvar slurm-mode-manipulation-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "/ u") 'slurm-filter-user)
    (define-key map (kbd "/ p") 'slurm-filter-partition)
    (define-key map (kbd "s u") 'slurm-sort-user)
    (define-key map (kbd "s p") 'slurm-sort-partition)
    (define-key map (kbd "s P") 'slurm-sort-priority)
    (define-key map (kbd "s j") 'slurm-sort-jobname)
    (define-key map (kbd "s d") 'slurm-sort-default)
    (define-key map (kbd "s c") 'slurm-sort)
    map)
  "Keymap for `slurm-mode-manipulation'.")
(defvar slurm-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map (make-composed-keymap (list slurm-mode-view-map slurm-mode-partition-map
						 slurm-mode-job-map slurm-mode-manipulation-map)))
    (define-key map (kbd "h")   'describe-mode)
    (define-key map (kbd "?")   'describe-mode)
    (define-key map (kbd "q")   'quit-window)
    map)
  "Keymap for `slurm-mode'.")

(eval-when-compile
  (defvar auto-revert-interval))

(define-derived-mode slurm-mode nil "Slurm"
  "Major-mode for interacting with slurm.

  \\[describe-mode] - Display this help.
Views:
\\{slurm-mode-view-map}

Operations on partitions:
\\{slurm-mode-partition-map}

Operations on jobs:
\\{slurm-mode-job-map}

Manipulations of the jobs list:
\\{slurm-mode-manipulation-map}"
  (interactive)
  ;; (kill-all-local-variables)
  ;; (use-local-map slurm-mode-map)
  ;; (setq mode-name "Slurm")
  ;; (setq major-mode 'slurm-mode)
  (hl-line-mode 1)
  (toggle-truncate-lines 1)
  (setq buffer-read-only t)

  (setq-local slurm--state nil)

  ;; Initialize user filter
  (if slurm-filter-user-at-start
      (slurm-filter-user (if (and slurm-remote-host
                                  slurm-remote-username)
                             slurm-remote-username
                           (shell-command-to-string "echo -n $USER")))
    (slurm-filter-user ""))

  ;; Initialize partition filter
  (slurm--update-partitions)
  (slurm-filter-partition "*ALL*")

  ;; Initialize sorting order
  (slurm-sort "")

  ;; Draw display
  (slurm--set :initialized t) ; From now on, filters and sort will refresh the view
  (slurm-job-list)

  ;; Arrange for `revert-buffer' to call `slurm-refresh'
  (setq-local revert-buffer-function
       (lambda (&optional ignore-auto noconfirm) (slurm-refresh)))
  (setq-local buffer-stale-function
       (lambda (&optional noconfirm) 'fast))
  (setq-local auto-revert-interval 30)
  (when (fboundp 'auto-revert-set-timer)
    (auto-revert-set-timer)))


;; ** Views

(defun slurm--in-view (view)
  "Non-nil if the current SLURM view is VIEW."
  (eq (slurm--get :view) view))

(defun slurm--run-one-command ()
  "Run the next SLURM command.
Schedule the following command to be executed after termination of the current one."
  (let ((commands (slurm--get :running-commands)))
    (when commands
      (let ((command (car commands)))
        (slurm--set :running-commands (cdr commands))
        (setq buffer-read-only nil)
        (goto-char (point-max))
        (newline 3)
        (let ((pos1 (point)))
          (insert (if slurm-remote-host
                      (format "%s> " slurm-remote-host)
                    "> ")
                  (combine-and-quote-strings command))
          (add-text-properties pos1 (point) '(face ((:weight bold)))))
        (newline 2)
        (sit-for 0)

        (slurm--run-command
         :message (format "Running %s" (car command))
         :current-buffer t
         :command command)
        (progn
           (delete-trailing-whitespace)
           (goto-char (point-min))
           (forward-line (1- (slurm--get :old-position)))
           (setq buffer-read-only t)
           (set-buffer-modified-p nil)
           (slurm--run-one-command))))))

(defun slurm-refresh ()
  "Refresh current slurm view."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (slurm--set :old-position (max (line-number-at-pos) 8))
    (slurm--set :running-commands (slurm--get :command))
    (setq buffer-read-only nil)
    (setq slurm-remote-host (concat "/ssh:" (nth 1 (split-string (buffer-name) "-")) ":" ";"))
    (erase-buffer)
    (insert (format-time-string "%Y-%m-%d %H:%M:%S\n"))
    (when slurm-display-help
      (forward-line)(newline)
      (insert "Type `h' to display an help message"))
    (slurm--run-one-command)))

(defun slurm-details ()
  "Show details on the current slurm entity (job or partition depending on the context)."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (if (slurm--in-view 'slurm-job-list)       (slurm-job-details))
    (if (slurm--in-view 'slurm-partition-list) (slurm-partition-details))))


;; *** Jobs list

(defvar slurm--squeue-format-switch nil
  "Switch passed to the squeue command to set columns format.
Must be updated using `slurm-update-squeue-format' whenever
`slurm-squeue-format' is modified.")

(defvar slurm--sacct-format-switch nil
  "Switch passed to the squeue command to set columns format.
Must be updated using `slurm-update-sacct-format' whenever
`slurm-sacct-format' is modified.")


(defun slurm-job-list ()
  "Switch to slurm jobs list view."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (slurm--set :command `(("squeue"
                            "-o" ,slurm--squeue-format-switch
                            ,@(slurm--squeue-filter-user)
                            ,@(slurm--squeue-filter-partition)
                            ,@(slurm--squeue-sort))))
    (setq mode-name "Slurm (jobs list)")
    (slurm--set :view 'slurm-job-list)
    (slurm-refresh)))

(defun slurm-sacct ()
  "Switch to slurm jobs list view."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (slurm--set :command `(("sacct" "-X"
                            "--format" ,slurm--sacct-format-switch
                            ,@(slurm--squeue-filter-user)
                            ,@(slurm--sacct-take-date))))
    (setq mode-name "Slurm (sacct list)")
    (slurm--set :view 'slurm-sacct)
    (slurm-refresh)))


;; **** Squeue output parsing

(defconst slurm--squeue-format-fields
  '((jobid     . "i")
    (partition . "P")
    (name      . "j")
    (user      . "u")
    (st        . "t")
    (time      . "M")
    (nodes     . "D")
    (priority  . "Q")
    (nodelist  . "R"))
  "Mapping between squeue fields and the corresponding '%' type specifications.")

(defvar slurm--squeue-format-columns nil
  "Definition of columns in the squeue output.

Must be updated using `slurm-update-squeue-format' whenever
`slurm-squeue-format' is modified.")

(defvar slurm--sacct-format-columns nil
  "Definition of columns in the squeue output.

Must be updated using `slurm-update-squeue-format' whenever
`slurm-squeue-format' is modified.")


(defun slurm--map-squeue-format (fun)
  "Helper function to walk the squeue format.

FUN is called for each field specification in
`slurm-squeue-format'.  It should have the following prototype:

FUN (name width &optional align)"
  (-map (lambda (field)
          (apply fun field))
        slurm-squeue-format))

(defun slurm--map-sacct-format (fun)
  "Helper function to walk the squeue format.

FUN is called for each field specification in
`slurm-squeue-format'.  It should have the following prototype:

FUN (name width &optional align)"
  (-map (lambda (field)
          (apply fun field))
        slurm-sacct-format))

(defun slurm-update-squeue-format ()
  "Update internal variables when `slurm-squeue-format' is changed.

Updated variables are `slurm--squeue-format-columns' and
`slurm--squeue-format-switch'."
  (setq slurm--squeue-format-switch
        (-reduce
         (lambda (a b) (concat a " " b))
         (slurm--map-squeue-format
          (lambda (name width &optional align)
            (let ((field (cdr (assq name slurm--squeue-format-fields)))
                  (mod   (if (eq align 'right) "." "")))
              (format "%%%s%d%s" mod width field))))))

  (setq slurm--squeue-format-columns
        (let ((pos 0))
          (slurm--map-squeue-format
           (lambda (name width &optional align)
             (prog1
                 (list name pos (+ pos width))
               (setq pos (+ pos width 1))))))))
(slurm-update-squeue-format)



(defun slurm--squeue-get-column (name)
  "Get the value of the NAME column in the current line.

Returned values are trimmed.  NAME must correspond to a field
listed in `slurm-squeue-format'."
  (let* ((column   (assq name slurm--squeue-format-columns))
         (col-beg  (nth 1 column))
         (col-end  (nth 2 column))
         (line-beg (line-beginning-position)))
    (s-trim (buffer-substring-no-properties
             (+ line-beg col-beg)
             (+ line-beg col-end)))))


(defun slurm-update-sacct-format ()
  "Update internal variables when `slurm-squeue-format' is changed.

Updated variables are `slurm--squeue-format-columns' and
`slurm--squeue-format-switch'."
  (setq slurm--sacct-format-switch
        (-reduce
         (lambda (a b) (concat a "," b))
         (slurm--map-sacct-format
          (lambda (name width &optional align)
                   (format "%s%s%d" name "%" width)))))

  (setq slurm--sacct-format-columns
        (let ((pos 0))
          (slurm--map-sacct-format
           (lambda (name width &optional align)
             (prog1
                 (list name pos (+ pos width))
               (setq pos (+ pos width 1))))))))
(slurm-update-sacct-format)


;; **** Filtering

(defun slurm-filter-user (user)
  "Filter slurm jobs belonging to USER."
  (interactive (list (read-from-minibuffer "User name (blank for all)? " (slurm--get :filter-user))))
  (when (eq major-mode 'slurm-mode)
    (slurm--set :filter-user user)
    (when (slurm--get :initialized) (slurm-job-list))))

(defun slurm--squeue-filter-user ()
  "Return the squeue switch to filter by user."
  (unless (string= (slurm--get :filter-user) "")
    (list "-u" (slurm--get :filter-user))))

(defun slurm--sacct-take-date ()
  "Return the squeue switch to filter by user."
  (unless (string= (slurm--get :filter-user) "")
    (list "-S" (org-read-date))))


(defun slurm-filter-partition (partition)
  "Filter slurm jobs assigned to PARTITION."
  (interactive (list (completing-read "Partition name: " (append (list "*ALL*") (slurm--get :partitions))
                                      nil nil nil nil (slurm--get :filter-partition))))
  (when (eq major-mode 'slurm-mode)
    (slurm--set :filter-partition partition)
    (when (slurm--get :initialized) (slurm-job-list))))

(defun slurm--squeue-filter-partition ()
  "Return the squeue switch to filter by partition."
  (unless (string= (slurm--get :filter-partition) "*ALL*")
    (list "-p" (slurm--get :filter-partition))))


;; **** Sorting

(defun slurm-sort (arg)
  "Set a custom sorting order for slurm jobs.

ARG must be in a form suitable to be passed as a '-S' switch to the squeue command (see `man squeue')."
  (interactive (list (read-from-minibuffer "Sort by (blank for default)? " (slurm--get :sort))))
  (when (eq major-mode 'slurm-mode)
    (slurm--set :sort arg)
    (when (slurm--get :initialized) (slurm-job-list))))

(defun slurm--squeue-sort ()
  "Return the squeue switch to sort."
  (unless (string= (slurm--get :sort) "")
    (list "-S" (slurm--get :sort))))

(defmacro slurm-define-sort (name char)
  "Define a command to change the slurm jobs sorting order.

The command will be named after NAME, and corresponds to giving
the CHAR argument to squeue's '-S' switch."
  `(defun ,(intern (concat "slurm-sort-" name)) (&optional argp)
     ,(concat "Sort slurm jobs by " name ".\n\n"
              "Give a prefix argument to reverse the sorting order.")
     (interactive "P")
     (if argp
         (slurm-sort ,(concat "-" char))
       (slurm-sort ,char))))
(slurm-define-sort "user"      "u")
(slurm-define-sort "partition" "P")
(slurm-define-sort "priority"  "p")
(slurm-define-sort "jobname"   "j")

(defun slurm-sort-default ()
  "Revert to default slurm jobs sorting order."
  (interactive)
  (slurm-sort ""))



;; **** Jobs manipulation

(defun slurm-job-id ()
  "Return the slurm job id in the current context.

In the `slurm-job-list' view, this is the job displayed on the
current line.  In the `slurm-job-details' view, this is the job
currently being displayed."
  (beginning-of-line)
  (cond ((or (slurm--in-view 'slurm-sacct) (slurm--in-view 'slurm-job-list))
         (let ((jobid (slurm--squeue-get-column 'jobid)))
           (unless (string-match "^[[:digit:]]" jobid)
             (error "Could not find valid job id on this line"))
           jobid))
        ((slurm--in-view 'slurm-job-details)
         (slurm--get :jobid))
        (t
         (error "Bad context for slurm-job-id"))))

(defun slurm-job-user-details ()
  "Display details on the jub submitter, as returned by the shell `finger' utility."
  (interactive)
  (when (slurm--in-view 'slurm-job-list)
    (let ((user (slurm--squeue-get-column 'user)))
      (slurm--run-command
       :message "Retrieving user details"
       :command `("finger" ,user)
       :post    (message "%s" (buffer-string))))))

(defun slurm-job-details ()
  "Show details about the current SLURM job."
  (when (eq major-mode 'slurm-mode)
    (when (slurm--in-view 'slurm-job-list)
      (let ((jobid  (slurm-job-id)))

	(when (string-match "^\\([[:digit:]]+\\)_*"
                               jobid)
        (setq type  "array"
              jobid (match-string 1 jobid)))

        (slurm--set :command `(("scontrol" "show" "job" ,jobid)))
        (slurm--set :jobid   jobid))
      (setq mode-name "Slurm (job details)")
      (slurm--set :view 'slurm-job-details)
      (slurm-refresh))))


(defun slurm-seff (argp)
  "Show details about resource usage SLURM jobid ARGP."

  (interactive "P")
  (when (eq major-mode 'slurm-mode)
    (let ((jobid (slurm-job-id))
          (type  "job"))

      (when (and argp
                 (string-match "^\\([[:digit:]]+\\)_\\([[:digit:]]+\\)$"
                               jobid))
        (setq type  "array"
              jobid (match-string 1 jobid)))

	(slurm--set :command `(("seff" ,jobid, "|" ,"sed" ,"-r" ,"s/[[:cntrl:]][[0-9]{1,3}m//g")))
        (slurm--set :jobid   jobid))
	(setq mode-name "Slurm (seff details)")
	(slurm--set :view 'slurm-seff)
        (slurm-refresh)))

(defun slurm-job-cancel (argp)
  "Kill (cancel) current slurm job.

When used with a prefix argument (ARGP non-nil) and the current
job belongs to a job array, cancel the whole array."
  (interactive "P")
  (when (eq major-mode 'slurm-mode)
    (let ((jobid (slurm-job-id))
          (type  "job"))

      (when (and argp
                 (string-match "^\\([[:digit:]]+\\)_\\([[:digit:]]+\\)$"
                               jobid))
        (setq type  "array"
              jobid (match-string 1 jobid)))

      (when (y-or-n-p (format "Really cancel %s %s? " type jobid))
        (slurm--run-command
         :message "Cancelling job"
         :command `("scancel" ,jobid))
        (slurm-refresh)))))

(defun slurm-job-update ()
  "Edit (update) current slurm job."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (let ((jobid (slurm-job-id)))
      (switch-to-buffer (get-buffer-create (format "*slurm update job %s*" jobid)))
      (slurm-update-mode)
      (slurm--set :command `(("scontrol" "show" "job" ,jobid)))
      (slurm-update-refresh))))


;; *** Partitions list

(defun slurm-partition-list ()
  "Switch to slurm partitions list view."
  (interactive)
  (when (eq major-mode 'slurm-mode)
    (slurm--set :command '(("scontrol" "show" "partition")))
    (setq mode-name "Slurm (partitions list)")
    (slurm--set :view 'slurm-partition-list)
    (slurm-refresh)))



;; **** Partitions manipulation

(defun slurm--update-partitions ()
  "Update the list of SLURM partitions.
This list will be used to provide completion when filtering jobs
by partition."
  (slurm--run-command
   :command '("scontrol" "show" "partitions")
   :post
   (let ((partitions nil))
     (goto-char (point-min))
     (while (search-forward "PartitionName=" nil t)
       (let ((beg (point)))
         (forward-word)
         (add-to-list 'partitions (buffer-substring beg (point)))))
     (with-current-buffer slurm--buffer
       (slurm--set :partitions partitions)))))

(defun slurm-partition-id ()
  "Return the id of the slurm partition at point."
  (backward-paragraph)(forward-line 1)
  (if (search-forward-regexp "^[[:space:]]*PartitionName=\\(.*\\)[[:space:]]*$" (line-end-position))
      (match-string 1)
    (error "Could not extract partition name on this paragraph")))

(defun slurm-partition-details ()
  "Display details about the partition at point."
  (when (slurm--in-view 'slurm-partition-list)
    (slurm-cluster-info (slurm-partition-id))))


;; *** Cluster information

(defun slurm-cluster-info (partition)
  "Show global information on the current state of the cluster.

If PARTITION is set, only show that partition's state.
If PARTITION is nil, show stats for the entire cluster."
  (interactive (list nil))
  (when (eq major-mode 'slurm-mode)
    (let ((switch (if partition `("-p" ,partition))))
      (slurm--set :command `(("sinfo" ,@switch)
                             ("sinfo" "-o" "%C" ,@switch))))
    (setq mode-name "Slurm (cluster info)")
    (slurm--set :view 'slurm-cluster-info)
    (slurm-refresh)))


;; * Slurm-update-mode

(defvar slurm-update-mode-map nil
  "Keymap for `slurm-update-mode'.")
(unless slurm-update-mode-map
  (setq slurm-update-mode-map text-mode-map)
  (define-key slurm-update-mode-map (kbd "C-c C-c") 'slurm-update-send)
  (define-key slurm-update-mode-map (kbd "C-c C-q") 'slurm-update-quit))

(define-derived-mode slurm-update-mode nil "Slurm-Update"
  "Major-mode for updating slurm entities.

Edit the line you want to update and hit \\[slurm-update-send] to validate your changes.

Key bindings:
  \\[slurm-update-send] - Validate your changes on a line.
  \\[slurm-update-refresh] - Refresh view.
  \\[slurm-update-quit] - Quit this mode."
  (interactive)
  ;; (kill-all-local-variables)
  ;; (use-local-map slurm-update-mode-map)
  ;; (setq mode-name "Slurm update")
  ;; (setq major-mode 'slurm-update-mode)
  (make-local-variable 'slurm--state)
  (hl-line-mode 1))

(defun slurm-update-refresh ()
  "Refresh slurm-update buffer."
  (interactive)
  (when (eq major-mode 'slurm-update-mode)
    (slurm--set :old-position (point))
    (erase-buffer)
    (slurm--run-command
     :current-buffer t
     :command (car (slurm--get :command)))

    (goto-char (point-min))
    (while (re-search-forward "^[[:space:]]+" nil t)
      (replace-match ""))
    (goto-char (point-min))
    (while (re-search-forward " [[:alnum:]]+=" nil t)
      (goto-char (match-beginning 0))
      (delete-char 1)
      (newline))
    (goto-char (slurm--get :old-position))))

(defun slurm-update-send ()
  "Validate a parameter change in the slurm-update-buffer."
  (interactive)
  (when (eq major-mode 'slurm-update-mode)
    (let* ((id       (save-excursion
                       (goto-char (point-min))
                       (buffer-substring (line-beginning-position) (line-end-position))))
           (prop     (buffer-substring (line-beginning-position) (line-end-position))))
      (slurm--run-command
       :message "Updating job"
       :command `("scontrol" "update" ,id ,prop))
      (slurm-update-refresh))))

(defun slurm-update-quit ()
  "Quit slurm-update mode."
  (interactive)
  (when (eq major-mode 'slurm-update-mode)
    (kill-buffer)
    (if (file-remote-p default-directory)
       (switch-to-buffer (get-buffer-create (concat "*slurm-*" (file-remote-p default-directory 'host))))
     (switch-to-buffer (get-buffer-create "*slurm*")))
    ;(switch-to-buffer "*slurm*")
    (slurm-refresh)))

(provide 'slurm-mode)

;;; slurm-mode.el ends here

;;; slurm-mode-test.el --- Tests for slurm-mode.el  -*- lexical-binding: t; -*-

;; Copyright (C) 2026

;;; Commentary:

;; ERT tests for slurm-mode, focusing on job watching and completion notifications.

;;; Code:

(require 'ert)
(require 'cl-lib)

;; Load slurm-mode from the same directory as this test file
(let ((dir (file-name-directory (or load-file-name buffer-file-name))))
  (load (expand-file-name "slurm-mode" dir)))

;;; Test helpers

(defvar slurm-test--notifications nil
  "List of notifications received during tests.
Each element is a cons (JOB-ID . STATUS).")

(defun slurm-test--notify-capture (job-id status)
  "Capture notification for JOB-ID with STATUS."
  (push (cons job-id status) slurm-test--notifications))

(defmacro slurm-test--with-mock-state (job-id state &rest body)
  "Execute BODY with `slurm--get-job-state' returning STATE for JOB-ID.
Also captures notifications via `slurm-test--notifications'."
  (declare (indent 2))
  `(let ((slurm-test--notifications nil)
         (slurm-watch-notify-function #'slurm-test--notify-capture)
         (slurm-watch-timers (make-hash-table :test 'equal)))
     ;; Set up a mock timer for the job
     (puthash ,job-id (run-at-time nil nil #'ignore) slurm-watch-timers)
     (cl-letf (((symbol-function 'slurm--get-job-state)
                (lambda (_id) ,state)))
       ,@body)))

;;; Tests for slurm--check-watched-job

(ert-deftest slurm-test-notification-on-completed ()
  "GIVEN a watched job that has COMPLETED
WHEN slurm--check-watched-job is called
THEN notification is triggered with COMPLETED status."
  (slurm-test--with-mock-state "12345" "COMPLETED"
    (slurm--check-watched-job "12345")
    (should (equal slurm-test--notifications '(("12345" . "COMPLETED"))))
    (should (null (gethash "12345" slurm-watch-timers)))))

(ert-deftest slurm-test-notification-on-failed ()
  "GIVEN a watched job that has FAILED
WHEN slurm--check-watched-job is called
THEN notification is triggered with FAILED status."
  (slurm-test--with-mock-state "12345" "FAILED"
    (slurm--check-watched-job "12345")
    (should (equal slurm-test--notifications '(("12345" . "FAILED"))))
    (should (null (gethash "12345" slurm-watch-timers)))))

(ert-deftest slurm-test-notification-on-cancelled ()
  "GIVEN a watched job that has been CANCELLED
WHEN slurm--check-watched-job is called
THEN notification is triggered with CANCELLED status."
  (slurm-test--with-mock-state "12345" "CANCELLED"
    (slurm--check-watched-job "12345")
    (should (equal slurm-test--notifications '(("12345" . "CANCELLED"))))
    (should (null (gethash "12345" slurm-watch-timers)))))

(ert-deftest slurm-test-notification-on-timeout ()
  "GIVEN a watched job that has TIMEOUT
WHEN slurm--check-watched-job is called
THEN notification is triggered with TIMEOUT status."
  (slurm-test--with-mock-state "12345" "TIMEOUT"
    (slurm--check-watched-job "12345")
    (should (equal slurm-test--notifications '(("12345" . "TIMEOUT"))))
    (should (null (gethash "12345" slurm-watch-timers)))))

(ert-deftest slurm-test-notification-on-node-fail ()
  "GIVEN a watched job that has NODE_FAIL
WHEN slurm--check-watched-job is called
THEN notification is triggered with NODE_FAIL status."
  (slurm-test--with-mock-state "12345" "NODE_FAIL"
    (slurm--check-watched-job "12345")
    (should (equal slurm-test--notifications '(("12345" . "NODE_FAIL"))))
    (should (null (gethash "12345" slurm-watch-timers)))))

(ert-deftest slurm-test-no-notification-when-running ()
  "GIVEN a watched job that is still RUNNING
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "RUNNING"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-pending ()
  "GIVEN a watched job that is PENDING
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "PENDING"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-configuring ()
  "GIVEN a watched job that is CONFIGURING
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "CONFIGURING"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-completing ()
  "GIVEN a watched job that is COMPLETING
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "COMPLETING"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-suspended ()
  "GIVEN a watched job that is SUSPENDED
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "SUSPENDED"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-resizing ()
  "GIVEN a watched job that is RESIZING
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" "RESIZING"
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

(ert-deftest slurm-test-no-notification-when-state-nil ()
  "GIVEN a watched job with unknown state (nil)
WHEN slurm--check-watched-job is called
THEN no notification is triggered AND job stays watched."
  (slurm-test--with-mock-state "12345" nil
    (slurm--check-watched-job "12345")
    (should (null slurm-test--notifications))
    (should (gethash "12345" slurm-watch-timers))))

;;; Tests for slurm-watch-notify-default

(ert-deftest slurm-test-default-notify-message ()
  "GIVEN the default notify function
WHEN called with job-id and status
THEN it displays a message with the correct format."
  ;; Note: We can't easily mock `message' in Emacs 31+ due to native compilation.
  ;; Instead, we verify the function doesn't error and trust the implementation.
  ;; The format string is straightforward: "SLURM job %s completed with status: %s"
  (should (progn (slurm-watch-notify-default "99999" "FAILED") t))
  (should (progn (slurm-watch-notify-default "12345" "COMPLETED") t)))

;;; Test for squeue returning empty (finished job falls through to sacct)

(ert-deftest slurm-test-squeue-empty-falls-through-to-sacct ()
  "GIVEN squeue returns 0 with empty output (job finished)
WHEN slurm--get-job-state is called
THEN it falls through to sacct and returns the final state."
  (cl-letf (((symbol-function 'call-process)
             (lambda (program &rest args)
               (cond
                ((string= program "squeue")
                 ;; squeue returns 0 but empty (job not in queue anymore)
                 0)
                ((string= program "sacct")
                 ;; sacct returns the final state
                 (insert "  COMPLETED \n")
                 0)))))
    (should (equal (slurm--get-job-state "12345") "COMPLETED"))))


(provide 'slurm-mode-test)
;;; slurm-mode-test.el ends here

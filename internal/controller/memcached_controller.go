/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/andrewbytecoder/memcached/api/v1alpha1"
)

const (
	// typeAvailableMemcached 表示部署调整的状态
	typeAvailableMemcached = "Available"
)

// MemcachedReconciler reconciles a Memcached object
type MemcachedReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//RBAC 权限 现在通过 RBAC 标记 进行配置，这些标记用于生成和更新位于 config/rbac/
// 中的清单文件。这些标记可以在每个控制器的 Reconcile() 方法中找到（并应在此处定义），请查看我们示例中的实现方式：
//在对控制器进行更改后，运行 make generate 命令。这样会提示 controller-gen 刷新位于 config/rbac 下的文件。

// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.example.com,resources=memcacheds/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile 是主 Kubernetes 调整循环的一部分，旨在将集群的当前状态向期望状态靠拢.
// 控制器的调整循环必须是幂等的。遵循操作员模式，您将创建提供调整功能的控制器，
// 该功能负责同步资源，直到集群达到期望状态。
// 违反此建议会违背控制器运行时的设计原则，可能导致意想不到的后果，例如资源卡住并需要手动干预。
// 了解更多信息：
// - 关于操作员模式：https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
// - 关于控制器：https://kubernetes.io/docs/concepts/architecture/controller/
//
// 有关更多详细信息，请查看 Reconcile 及其结果：
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *MemcachedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	// 获取memcached实例
	// 目的是检查 kind Memcached 的自定义资源是否已经在集群上应用，如果没有应用我们返回nil，以停止对账过程
	memcached := &cachev1alpha1.Memcached{}
	err := r.Get(ctx, req.NamespacedName, memcached)
	if err != nil {
		log.Error(err, "unable to fetch Memcached")
		if apierrors.IsNotFound(err) {
			//	 如果资源不存在，通常说明自定义资源被删除，或者还没有创建
			log.Info("memcached resource not found. Ignoring since object must be deleted")
			// 停止对账，停止调整过程
			return ctrl.Result{}, nil
		}
	}

	// 当没有可用状态时，我们将状态设置为未知
	if len(memcached.Status.Conditions) == 0 {
		meta.SetStatusCondition(&memcached.Status.Conditions,
			metav1.Condition{Type: typeAvailableMemcached, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, memcached); err != nil {
			log.Error(err, "unable to update Memcached status")
			return ctrl.Result{}, err
		}

		//	 再更新状态之后，我们重新获取 Memcached自定义资源
		//	 这样我们就可以件获取得到集群中资源最新状态，避免银帆错误 对象已被修改，请将您的更改应用于最新版本并重试
		// 这将在后续的操作中,如果我们再次尝试更新它,将重新触发对账过程
		if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
			log.Error(err, "unable to fetch Memcached")
			return ctrl.Result{}, err
		}
	}

	// 检查部署是否已经存在，如果不存在则创建一个新部署。
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// 定义新的部署
		dep, err := r.deploymentForMemcached(memcached)
		if err != nil {
			log.Error(err, "Failed to define new Deployment resource for Memcached")

			// 以下实现将更新状态
			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{Type: typeAvailableMemcached,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create Deployment for the custom resource (%s): (%s)", memcached.Name, err)})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		log.Info("Creating a new Deployment",
			"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			log.Error(err, "无法创建新的部署",
				"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return ctrl.Result{}, err
		}

		// 部署成功创建
		// 我们将重新排队调整，以确保状态
		// 并继续进行下一步操作
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Deployment")
		// 让我们返回错误，以便重新触发对账。
		return ctrl.Result{}, err
	}

	// CRD API 定义了 Memcached 类型具有一个 MemcachedSpec.Size 字段
	// 用于设置集群中部署实例的数量到所需状态。
	// 因此，以下代码将确保部署的大小与我们正在对账的
	// 自定义资源的 Size 规格中定义的相同。
	size := memcached.Spec.Size
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		if err = r.Update(ctx, found); err != nil {
			log.Error(err, "Failed to update Deployment",
				"Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)

			// 在更新状态之前重新获取 memcached 自定义资源，
			// 以便我们能够获得集群中资源的最新状态，并避免
			// 引发错误"对象已被修改，请将您的更改应用于
			// 最新版本并重试"，这将重新触发调整。
			if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
				log.Error(err, "Failed to re-fetch memcached")
				return ctrl.Result{}, err
			}

			// 以下实现将更新状态
			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{Type: typeAvailableMemcached,
				Status: metav1.ConditionFalse, Reason: "Resizing",
				Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", memcached.Name, err)})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		// 现在我们更新了大小，我们想要重新排队对账
		// 以便确保在更新之前我们拥有资源的最新状态。
		// 此外，它还将有助于确保集群上的期望状态。
		return ctrl.Result{Requeue: true}, nil
	}

	// 以下实现将更新状态
	meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{Type: typeAvailableMemcached,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("自定义资源（%s）部署成功，创建了 %d 个副本", memcached.Name, size)})

	if err := r.Status().Update(ctx, memcached); err != nil {
		log.Error(err, "Failed to update Memcached status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager 将控制器与管理器进行设置。
func (r *MemcachedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// 监控 Memcached 自定义资源，并在其创建更新或删除的时候触发一致性检查
		For(&cachev1alpha1.Memcached{}).
		// 监视由 Memcached 控制器管理的 Deployment。如果该控制器拥有和管理的 Deployment 发生任何变化，将触发对账，确保集群状态与期望状态一致。
		Owns(&appsv1.Deployment{}).
		Named("memcached").
		Complete(r)
}

// deploymentForMemcached 返回一个 Memcached 部署对象。
func (r *MemcachedReconciler) deploymentForMemcached(
	memcached *cachev1alpha1.Memcached) (*appsv1.Deployment, error) {
	replicas := memcached.Spec.Size
	image := "memcached:1.6.26-alpine3.19"

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcached.Name,
			Namespace: memcached.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "project"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app.kubernetes.io/name": "project"},
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "memcached",
						ImagePullPolicy: corev1.PullIfNotPresent,
						// 确保容器的限制性上下文
						// 详细信息请参考： https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             ptr.To(true),
							RunAsUser:                ptr.To(int64(1001)),
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{
									"ALL",
								},
							},
						},
						Ports: []corev1.ContainerPort{{
							ContainerPort: 11211,
							Name:          "memcached",
						}},
						Command: []string{"memcached", "--memory-limit=64", "-o", "modern", "-v"},
					}},
				},
			},
		},
	}

	// 设置 Deployment 的 ownerRef
	// 更多信息： https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
	if err := ctrl.SetControllerReference(memcached, dep, r.Scheme); err != nil {
		return nil, err
	}
	return dep, nil
}
